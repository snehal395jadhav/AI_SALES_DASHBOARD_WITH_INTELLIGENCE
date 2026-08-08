"""
Market Information — market_info.py
====================================
Fetches structured market data for a country + product category combination:
  • Market size (total, domestic, imports, YoY growth)
  • Major importers (companies / buyers, top 10)
  • Major exporting countries to the market
  • Duties & tariffs (BCD, ADD, CVD, FTA rates)
  • India's position (share, rank, trend)

AI provider: NVIDIA NIM (nemotron-3-ultra-550b-a55b) is tried first; if that
fails, falls back to OpenRouter models with structured JSON output.

Required env vars: NVIDIA_API_KEY, OPENROUTER_API_KEY, TAVILY_API_KEY
Optional env vars: NVIDIA_MODEL (default nvidia/nemotron-3-ultra-550b-a55b),
                   MI_MODEL (default openai/gpt-oss-20b:free)
"""

import os
import json
import datetime as dt
import logging
import time
from contextlib import closing
import sqlite3
import requests

from env_utils import load_env_file

load_env_file(__file__)

logger = logging.getLogger("market_info")

BASE_DIR           = os.path.dirname(os.path.abspath(__file__))
DATA_DIR           = os.environ.get("DATA_DIR", os.path.join(BASE_DIR, "data"))
if not os.path.isabs(DATA_DIR):
    DATA_DIR = os.path.join(BASE_DIR, DATA_DIR)
DB_PATH            = os.path.join(DATA_DIR, "dashboard.db")
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")
NVIDIA_API_KEY     = os.environ.get("NVIDIA_API_KEY", "")
TAVILY_API_KEY     = os.environ.get("TAVILY_API_KEY", "")
OPENROUTER_URL     = "https://openrouter.ai/api/v1/chat/completions"
NVIDIA_URL         = "https://integrate.api.nvidia.com/v1/chat/completions"
AI_MODEL           = os.environ.get("MI_MODEL", "openai/gpt-oss-20b:free")
NVIDIA_MODEL        = os.environ.get("NVIDIA_MODEL", "nvidia/nemotron-3-ultra-550b-a55b")
FALLBACK_MODEL     = "nvidia/nemotron-3-nano-30b-a3b:free"
REQUEST_TIMEOUT    = 90
CACHE_DAYS         = 15
MAX_ATTEMPTS       = 7


def get_db():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    return db


def _parse_ai_json(raw: str) -> dict:
    raw = (raw or "").strip()
    if not raw:
        raise RuntimeError("AI returned an empty response instead of JSON")
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        start = raw.find("{")
        end = raw.rfind("}")
        if start >= 0 and end > start:
            try:
                parsed = json.loads(raw[start:end + 1])
            except json.JSONDecodeError:
                parsed = None
            else:
                if isinstance(parsed, dict):
                    return parsed
        preview = raw[:180].replace("\n", " ")
        raise RuntimeError(f"AI returned invalid JSON ({exc.msg}). Preview: {preview}") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError("AI returned JSON, but the payload was not an object")
    return parsed


def _is_rate_limit_error(exc: Exception) -> bool:
    response = getattr(exc, "response", None)
    if getattr(response, "status_code", None) == 429:
        return True
    text = str(exc).lower()
    return "429" in text or "too many requests" in text or "rate limit" in text


# ── Tavily search ─────────────────────────────────────────────────────────────

def _tavily_search(query: str, max_results: int = 6) -> list[dict]:
    if not TAVILY_API_KEY:
        return []
    try:
        resp = requests.post(
            "https://api.tavily.com/search",
            json={"api_key": TAVILY_API_KEY, "query": query,
                  "max_results": max_results, "search_depth": "advanced"},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json().get("results", [])
    except Exception as exc:
        logger.warning("Tavily search failed for %r: %s", query, exc)
        return []


def _gather_context(country: str, category: str, hs_codes: list | None = None,
                    products: list | None = None) -> str:
    hs_str = (" HS " + " ".join(hs_codes)) if hs_codes else ""
    prod_str = (", ".join(products[:4])) if products else f"{category} subcategory product segments"
    queries = [
        f"{category}{hs_str} import market size {country} USD value statistics",
        f"{prod_str} market breakdown {country} value share import",
        f"top companies distributors retailers wholesalers importing {category} into {country} market leaders buyers",
        f"top exporting countries {category}{hs_str} to {country} market share",
        f"{category}{hs_str} import duty tariff {country} customs BCD anti-dumping",
        f"India exports {category}{hs_str} to {country} market share rank value",
    ]
    snippets = []
    for q in queries:
        results = _tavily_search(q, max_results=4)
        for r in results:
            title = r.get("title", "")
            content = r.get("content", "")[:600]
            url = r.get("url", "")
            if content:
                snippets.append(f"[{title}] ({url})\n{content}")
        time.sleep(0.3)
    return "\n\n---\n\n".join(snippets[:20])


# ── OpenRouter AI call ────────────────────────────────────────────────────────

_SCHEMA_PROMPT = """
Return ONLY valid JSON (no markdown, no commentary) matching this exact schema:
{
  "market_size": {
    "total_usd_mn": <number or null>,
    "domestic_usd_mn": <number or null>,
    "domestic_pct": <number or null>,
    "imports_usd_mn": <number or null>,
    "imports_pct": <number or null>,
    "yoy_growth_pct": <number or null>,
    "data_year": <string e.g. "2023" or null>,
    "notes": <string>
  },
  "major_importers": [
    {
      "rank": <int>,
      "name": <string — MUST be a company/business name, NOT a country>,
      "hq_country": <string — the company's home country>,
      "import_value_usd_mn": <number or null>,
      "share_pct": <number or null>
    }
  ],
  "major_domestic_manufacturers": [
    {
      "rank": <int>,
      "name": <string — MUST be a company/business name that manufactures this category WITHIN {country}>,
      "production_value_usd_mn": <number or null — this manufacturer's domestic production value>,
      "share_of_domestic_pct": <number or null — this manufacturer's % share of the DOMESTIC PRODUCTION value only (market_size.domestic_usd_mn), NOT of total market. E.g. if domestic production is 38% of the total market and this manufacturer holds 25% of that domestic slice, share_of_domestic_pct = 25, NOT 25% of total market>,
      "notes": <string>
    }
  ],
  "major_exporters": [
    {
      "rank": <int>,
      "country": <string>,
      "export_value_usd_mn": <number or null>,
      "share_pct": <number or null>,
      "yoy_change_pct": <number or null>
    }
  ],
  "india_position": {
    "in_top5": <bool>,
    "rank": <int or null>,
    "export_value_usd_mn": <number or null>,
    "share_pct": <number or null>,
    "gap_to_top5_usd_mn": <number or null>,
    "trend": <"growing"|"declining"|"stable"|null>,
    "trend_note": <string>
  },
  "india_category_breakdown": [
    {
      "subcategory": <string — product sub-segment India exports>,
      "export_value_usd_mn": <number or null — India's export value for this sub-segment to target country>,
      "share_of_india_exports_pct": <number or null — % of India's total exports in this category to this country>,
      "yoy_growth_pct": <number or null>,
      "competitive_position": <"strong"|"moderate"|"weak"|null>,
      "notes": <string>
    }
  ],
  "duties_tariffs": {
    "bcd_pct": <number or null>,
    "social_welfare_surcharge_pct": <number or null>,
    "igst_vat_pct": <number or null>,
    "anti_dumping_pct": <number or null>,
    "anti_dumping_note": <string>,
    "countervailing_pct": <number or null>,
    "effective_total_pct": <number or null>,
    "fta_india_pct": <number or null>,
    "fta_note": <string>,
    "customs_portal_url": <string or null>
  },
  "subcategory_breakdown": [
    {
      "name": <string — subcategory / product segment name>,
      "market_size_usd_mn": <number or null — total market value for this sub-segment>,
      "share_pct": <number or null — % share of total category market>,
      "yoy_growth_pct": <number or null>,
      "import_share_pct": <number or null — % of this subcategory that is imported vs domestic>,
      "notes": <string>
    }
  ],
  "data_sources": [<string>],
  "confidence": <"high"|"medium"|"low">
}
Populate with the best data available from the context. Use null for unknown values.
subcategory_breakdown: list up to 10 product sub-segments or types within the main category, ordered by market size descending.
india_category_breakdown: list up to 8 sub-segments showing India's export composition to the target country for this category.
CRITICAL — major_importers: list up to 10 COMPANIES (retailers, distributors, wholesalers, industrial buyers) that physically import this product category INTO the target country. "name" must be a company/business name — NEVER a country name. "hq_country" is where the company is headquartered.
CRITICAL — major_exporters: list up to 10 SOURCE COUNTRIES that export this product TO the target country (i.e. countries from which the target country imports). NEVER include the target country itself in this list. Always include India even if not in top 5.
CRITICAL — major_domestic_manufacturers: list up to 8 COMPANIES that manufacture this product category WITHIN the target country (local/domestic production, not importers). share_of_domestic_pct is each manufacturer's share OF THE DOMESTIC PRODUCTION VALUE ONLY (market_size.domestic_usd_mn) — e.g. if domestic production is 38% of the total market and one manufacturer controls a quarter of that domestic output, share_of_domestic_pct = 25 (not 9.5, which would be 25% of the 38% expressed against the total market). The shares across all listed manufacturers should sum to roughly 100% of the domestic slice (or less if fragmented/long-tail remainder exists). If a country has negligible or no domestic manufacturing for this category, return an empty list.
"""


_SYSTEM_PROMPT = (
    "You are a Senior Economist and Trade Intelligence Analyst with 20+ years of experience, "
    "equivalent to a Partner-level expert at Deloitte or PwC. You have deep expertise in "
    "global trade flows, customs tariffs, import/export market dynamics, and industrial "
    "procurement. You rely on authoritative sources: UN Comtrade, ITC Trade Map, World Bank, "
    "USITC, national customs databases, and industry research reports. "
    "Your analysis is precise, data-driven, and explicitly distinguishes between companies "
    "(importers/buyers) and countries (source/export origins)."
)

_ANALYSIS_PROMPT = """You are analysing the {category} import market in {country}.

Using the research context provided, conduct a thorough economic analysis covering:
1. Total market size (domestic production + imports), import penetration, and YoY growth trend
2. Subcategory / product segment breakdown — how the overall {category} market in {country} is
   split across product types, segments, or end-use verticals (e.g. for "paper": copy paper,
   tissue, packaging, specialty; for "chemicals": industrial, specialty, agrochemical, etc.).
   Include market size, share %, growth, and import intensity per sub-segment.
3. Major COMPANIES that import {category} into {country} — these are retailers, distributors,
   industrial manufacturers, wholesalers, procurement arms. Name actual businesses, not countries.
4. Major COMPANIES that manufacture {category} domestically WITHIN {country} (local production,
   not importers). For each, estimate their share of the domestic production value (i.e. their
   share of the domestic-manufacturing slice of the market, not of the total market including
   imports). If {country} has negligible local manufacturing for this category, state that clearly.
5. Source COUNTRIES that export {category} TO {country} (countries from which {country} imports).
   Do NOT include {country} itself in this list.
6. Customs duties structure: BCD, anti-dumping, CVD, GST/VAT, effective total, any FTA rates
7. India's competitive position: rank, share, trend vs other exporters
8. India's category-wise export breakdown to {country}: which sub-segments India dominates or
   is weak in, with export value, share of India's total exports, YoY growth per sub-segment

Think through each section carefully using the data evidence in the context.
Cross-check figures for consistency (e.g. exporter shares should sum to ~100%).

Research Context:
{context}
"""

def _direct_call(url: str, headers: dict, model: str, messages: list,
                 temperature: float, max_tokens: int, timeout: int = REQUEST_TIMEOUT) -> str:
    """Non-streaming call — reliable fallback."""
    resp = requests.post(
        url,
        headers=headers,
        json={
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        },
        timeout=timeout,
    )
    resp.raise_for_status()
    try:
        body = resp.json()
    except ValueError as exc:
        preview = (resp.text or "")[:180].replace("\n", " ")
        raise RuntimeError(f"AI provider returned non-JSON response. Preview: {preview}") from exc
    # Surface API-level errors returned with HTTP 200
    if "error" in body:
        raise RuntimeError(body["error"].get("message", str(body["error"])))
    content = body["choices"][0]["message"].get("content") or ""
    return content


def _stream_call(url: str, headers: dict, model: str, messages: list,
                 temperature: float, max_tokens: int, timeout: int = REQUEST_TIMEOUT) -> str:
    """Streaming call; falls back to non-streaming if stream returns empty."""
    resp = requests.post(
        url,
        headers=headers,
        json={
            "model": model,
            "messages": messages,
            "stream": True,
            "temperature": temperature,
            "max_tokens": max_tokens,
        },
        stream=True,
        timeout=timeout,
    )
    resp.raise_for_status()
    content = ""
    last_chunk: dict = {}
    for raw_line in resp.iter_lines():
        if not raw_line:
            continue
        line = raw_line.decode("utf-8") if isinstance(raw_line, bytes) else raw_line
        if not line.startswith("data: "):
            continue
        payload = line[6:].strip()
        if payload == "[DONE]":
            break
        try:
            chunk = json.loads(payload)
            last_chunk = chunk
            delta = chunk["choices"][0].get("delta", {})
            piece = delta.get("content") or ""
            content += piece
        except Exception:
            continue

    # Some models send full content in the last chunk's message instead of deltas
    if not content.strip() and last_chunk:
        try:
            content = last_chunk["choices"][0].get("message", {}).get("content") or ""
        except Exception:
            pass

    return content


NVIDIA_TIMEOUT = 90  # fail fast — NVIDIA is tried once (stream only); fall back to OpenRouter if slow/down


def _call_with_fallback(messages: list, temperature: float, max_tokens: int) -> str:
    """Try NVIDIA first (stream only, fail fast), then OpenRouter models
    (streaming first, non-streaming retry)."""
    last_exc: Exception | None = None

    if NVIDIA_API_KEY:
        try:
            logger.info("Model: %s  stream=True", NVIDIA_MODEL)
            content = _stream_call(
                NVIDIA_URL,
                {"Authorization": f"Bearer {NVIDIA_API_KEY}", "Content-Type": "application/json"},
                NVIDIA_MODEL, messages, temperature, max_tokens, timeout=NVIDIA_TIMEOUT,
            )
            if content.strip():
                return content
            logger.warning("NVIDIA returned empty content — falling back to OpenRouter")
        except Exception as exc:
            logger.warning("NVIDIA call failed (%s) — falling back to OpenRouter", exc)
            last_exc = exc

    if not OPENROUTER_API_KEY:
        if last_exc:
            raise last_exc
        raise RuntimeError("Neither NVIDIA_API_KEY nor OPENROUTER_API_KEY is set")

    or_headers = {"Authorization": f"Bearer {OPENROUTER_API_KEY}", "Content-Type": "application/json"}
    models = [AI_MODEL]
    if FALLBACK_MODEL and FALLBACK_MODEL != AI_MODEL:
        models.append(FALLBACK_MODEL)

    for model in models:
        for use_stream in [True, False]:
            try:
                logger.info("Model: %s  stream=%s", model, use_stream)
                if use_stream:
                    content = _stream_call(OPENROUTER_URL, or_headers, model, messages, temperature, max_tokens)
                else:
                    content = _direct_call(OPENROUTER_URL, or_headers, model, messages, temperature, max_tokens)
                if content.strip():
                    return content
                logger.warning("Model %s returned empty content (stream=%s)", model, use_stream)
            except Exception as exc:
                logger.warning("Model %s stream=%s error: %s", model, use_stream, exc)
                last_exc = exc
                if _is_rate_limit_error(exc):
                    raise RuntimeError(
                        "OpenRouter is rate-limited right now. Using cached data where available; "
                        "wait a few minutes before refreshing uncached categories."
                    ) from exc
                time.sleep(1)

    raise RuntimeError(f"All models failed. Last error: {last_exc}")


def _ai_fetch(country: str, category: str, context: str, products: list | None = None) -> dict:
    if not NVIDIA_API_KEY and not OPENROUTER_API_KEY:
        raise RuntimeError("Neither NVIDIA_API_KEY nor OPENROUTER_API_KEY is set")

    # Build product constraint text if products are configured
    products = products or []
    if products:
        prod_list = "\n".join(f"  - {p}" for p in products)
        product_constraint = (
            f"\n\nIMPORTANT — SUBCATEGORY BREAKDOWN: The subcategory_breakdown section MUST use "
            f"exactly these product sub-segments (our actual product portfolio) and no others:\n"
            f"{prod_list}\n"
            f"Analyse each of these specific products within the {category} import market in {country}. "
            f"Do not substitute generic industry segments — use these names exactly."
        )
    else:
        product_constraint = ""

    turn1_user = _ANALYSIS_PROMPT.format(
        category=category, country=country, context=context
    ) + product_constraint

    # ── Turn 1: deep reasoning analysis (streamed) ────────────────────────────
    t1_messages = [
        {"role": "system", "content": _SYSTEM_PROMPT},
        {"role": "user",   "content": turn1_user},
    ]
    analysis = _call_with_fallback(t1_messages, temperature=0.2, max_tokens=3000)
    logger.info("Turn 1 analysis complete (%d chars)", len(analysis))

    # ── Turn 2: structured JSON output (streamed) ─────────────────────────────
    prod_rule = ""
    if products:
        prod_names = ", ".join(f'"{p}"' for p in products)
        prod_rule = (
            f"\n3. subcategory_breakdown → MUST contain exactly these products in the 'name' field: "
            f"{prod_names}. Use these exact names. Do not add, remove, or rename them."
        )

    turn2_user = (
        f"Based on your analysis above, now output the structured JSON.\n\n"
        f"STRICT RULES:\n"
        f"1. major_importers → COMPANY names only (retailers, distributors, buyers IN {country}). "
        f"Never put a country name in the 'name' field.\n"
        f"2. major_exporters → SOURCE countries that export TO {country}. "
        f"Never include '{country}' itself.\n"
        f"3. major_domestic_manufacturers → COMPANY names that manufacture {category} INSIDE {country} "
        f"(local production only, not importers/distributors). share_of_domestic_pct is each "
        f"company's share OF THE DOMESTIC PRODUCTION VALUE ONLY — i.e. if market_size.domestic_pct "
        f"is 38% of the total market, treat that 38% slice as its own 100% base and give each "
        f"manufacturer's percentage WITHIN that base (shares across manufacturers should sum to "
        f"~100% of the domestic slice). Do NOT express their share against the total market.\n"
        f"{prod_rule}\n\n"
        f"{_SCHEMA_PROMPT}"
    )
    t2_messages = [
        {"role": "system",    "content": _SYSTEM_PROMPT},
        {"role": "user",      "content": turn1_user},
        {"role": "assistant", "content": analysis},
        {"role": "user",      "content": turn2_user},
    ]
    raw = _call_with_fallback(t2_messages, temperature=0.1, max_tokens=2500)
    logger.info("Turn 2 JSON output complete (%d chars)", len(raw))

    return _parse_ai_json(raw)


# ── Public fetch function ─────────────────────────────────────────────────────

def _ai_fetch_fast(country: str, category: str, context: str, products: list | None = None) -> dict:
    """Single-turn version used for faster background refreshes."""
    if not NVIDIA_API_KEY and not OPENROUTER_API_KEY:
        raise RuntimeError("Neither NVIDIA_API_KEY nor OPENROUTER_API_KEY is set")

    products = products or []
    product_constraint = ""
    if products:
        prod_list = "\n".join(f"  - {p}" for p in products)
        product_constraint = (
            f"\nIMPORTANT — SUBCATEGORY BREAKDOWN: Use exactly these products in subcategory_breakdown, "
            f"with no extra segments:\n{prod_list}\n"
        )

    turn_user = (
        f"You are analysing the {category} import market in {country}.\n\n"
        f"Return ONLY valid JSON matching the schema below. Keep the response concise, factual, and "
        f"internally consistent. If unsure, use null rather than inventing detail.\n"
        f"major_importers must be company names, major_exporters must be source countries, and "
        f"major_domestic_manufacturers must be companies manufacturing inside {country}.\n"
        f"{product_constraint}\n"
        f"{_SCHEMA_PROMPT}\n\n"
        f"Research context:\n{context}"
    )
    messages = [
        {"role": "system", "content": _SYSTEM_PROMPT},
        {"role": "user", "content": turn_user},
    ]
    raw = _call_with_fallback(messages, temperature=0.1, max_tokens=2200)
    return _parse_ai_json(raw)


def fetch_market_info(country: str, category: str, hs_codes: list | None = None,
                      products: list | None = None, fast: bool = False) -> dict:
    """Fetch and return structured market data. Raises on failure."""
    context = _gather_context(country, category, hs_codes, products=products or [])
    data = _ai_fetch_fast(country, category, context, products=products or []) if fast else _ai_fetch(country, category, context, products=products or [])
    return data


# ── Cache helpers ─────────────────────────────────────────────────────────────

def get_cached(country: str, category: str) -> dict | None:
    """Return cached record or None."""
    with closing(get_db()) as db:
        row = db.execute(
            "SELECT * FROM market_info_cache WHERE country=? AND category=?",
            (country, category),
        ).fetchone()
    return dict(row) if row else None


def is_fresh(record: dict) -> bool:
    """True if fetch_date is within CACHE_DAYS."""
    try:
        fd = dt.date.fromisoformat(record["fetch_date"])
        return (dt.date.today() - fd).days <= CACHE_DAYS
    except Exception:
        return False


def save_cache(country: str, category: str, data: dict, triggered_by: str = "cron") -> None:
    now = dt.datetime.now().isoformat(timespec="seconds")
    today = dt.date.today().isoformat()
    next_update = (dt.date.today() + dt.timedelta(days=CACHE_DAYS)).isoformat()
    with closing(get_db()) as db:
        db.execute(
            """INSERT INTO market_info_cache
                 (country, category, data_json, fetch_date, next_update,
                  attempt_count, cache_status, created_at, updated_at)
               VALUES (?,?,?,?,?,0,'ok',?,?)
               ON CONFLICT(country,category) DO UPDATE SET
                 data_json=excluded.data_json,
                 fetch_date=excluded.fetch_date,
                 next_update=excluded.next_update,
                 attempt_count=0,
                 cache_status='ok',
                 updated_at=excluded.updated_at""",
            (country, category, json.dumps(data), today, next_update, now, now),
        )
        db.execute(
            """INSERT INTO market_info_refresh_log
                 (country, category, triggered_by, status, error_msg, refreshed_at)
               VALUES (?,?,?,'ok','',?)""",
            (country, category, triggered_by, now),
        )
        db.commit()


def record_failure(country: str, category: str, error: str) -> int:
    """Increment attempt_count, mark stale. Returns new attempt count."""
    now = dt.datetime.now().isoformat(timespec="seconds")
    with closing(get_db()) as db:
        db.execute(
            """INSERT INTO market_info_cache
                 (country, category, data_json, fetch_date, next_update,
                  attempt_count, cache_status, created_at, updated_at)
               VALUES (?,?,'{}',date('now'),date('now','+1 day'),1,'failed',?,?)
               ON CONFLICT(country,category) DO UPDATE SET
                 attempt_count=attempt_count+1,
                 cache_status='failed',
                 updated_at=excluded.updated_at""",
            (country, category, now, now),
        )
        db.execute(
            """INSERT INTO market_info_refresh_log
                 (country, category, triggered_by, status, error_msg, refreshed_at)
               VALUES (?,?,'cron','failed',?,?)""",
            (country, category, str(error)[:500], now),
        )
        db.commit()
        row = db.execute(
            "SELECT attempt_count FROM market_info_cache WHERE country=? AND category=?",
            (country, category),
        ).fetchone()
    return row["attempt_count"] if row else 1


def was_refreshed_today(country: str, category: str) -> bool:
    """True if a successful manual refresh happened in the last 24 hours."""
    cutoff = (dt.datetime.now() - dt.timedelta(hours=24)).isoformat(timespec="seconds")
    with closing(get_db()) as db:
        row = db.execute(
            """SELECT id FROM market_info_refresh_log
               WHERE country=? AND category=? AND triggered_by='manual'
                 AND status='ok' AND refreshed_at > ?
               LIMIT 1""",
            (country, category, cutoff),
        ).fetchone()
    return row is not None


# ── Scheduled refresh ─────────────────────────────────────────────────────────

def refresh_all_stale(alert_callback=None) -> None:
    """Called by cron: re-fetch every stale or failed cache entry."""
    with closing(get_db()) as db:
        rows = db.execute(
            """SELECT country, category, fetch_date, attempt_count, cache_status
               FROM market_info_cache""",
        ).fetchall()
        all_rows = [dict(r) for r in rows]

    for rec in all_rows:
        country, category = rec["country"], rec["category"]
        attempts = rec.get("attempt_count", 0)

        if attempts >= MAX_ATTEMPTS:
            logger.error(
                "Market info: %s/%s exceeded %d attempts — admin alert triggered",
                country, category, MAX_ATTEMPTS,
            )
            if alert_callback:
                alert_callback(country, category, attempts)
            continue

        if is_fresh(rec) and rec.get("cache_status") == "ok":
            continue

        try:
            data = fetch_market_info(country, category)
            save_cache(country, category, data, triggered_by="cron")
            logger.info("Market info refreshed: %s / %s", country, category)
        except Exception as exc:
            new_count = record_failure(country, category, str(exc))
            logger.warning(
                "Market info fetch failed (%d/%d): %s / %s — %s",
                new_count, MAX_ATTEMPTS, country, category, exc,
            )
            if new_count >= MAX_ATTEMPTS and alert_callback:
                alert_callback(country, category, new_count)
