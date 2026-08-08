"""
Monthly Market Intelligence — intelligence.py
=============================================
Fetches market intelligence via Tavily web search, then filters and
summarises results using OpenRouter AI (openai/gpt-oss-20b:free by default).

AI provider: NVIDIA NIM (nemotron-3-ultra-550b-a55b) is tried first; if that
call fails, falls back to OpenRouter (MI_MODEL).

Required environment variables
-------------------------------
  NVIDIA_API_KEY      — NVIDIA NIM API key (https://build.nvidia.com)
  OPENROUTER_API_KEY  — OpenRouter API key (https://openrouter.ai) — fallback
  TAVILY_API_KEY      — Tavily Search API key (https://tavily.com)
  DATA_DIR            — SQLite directory (default /app/data, shared with app.py)

Optional environment variables
-------------------------------
  NVIDIA_MODEL — NVIDIA NIM model ID (default: nvidia/nemotron-3-ultra-550b-a55b)
  MI_MODEL     — OpenRouter fallback model ID (default: openai/gpt-oss-20b:free)

Extending the system
---------------------
  Add teams/markets  : Admin UI → Market Intelligence → Team-Market Mapping
  Add categories     : Admin UI → Market Intelligence → Product Categories
  Add products       : Admin UI → inside any category row
  Change schedule    : edit the CronTrigger in app.py _start_scheduler()
  Change model       : set MI_MODEL env var in docker-compose.yml
"""

import os
import json
import hashlib
import datetime as dt
import logging
import time
from contextlib import closing
import sqlite3
import requests

from env_utils import load_env_file

load_env_file(__file__)

# ── Optional: Tavily SDK ──────────────────────────────────────────────────────
try:
    from tavily import TavilyClient as _TavilyClient
    HAS_TAVILY_SDK = True
except ImportError:
    HAS_TAVILY_SDK = False

logger = logging.getLogger("mi")

# ── Config ────────────────────────────────────────────────────────────────────
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
MAX_SEARCH_RESULTS    = 8   # Tavily results per query
MAX_UPDATES_PER_MARKET = 8  # AI hard cap per market
REQUEST_TIMEOUT    = 120    # seconds

# ── Search topics — drives Tavily query construction ─────────────────────────
SEARCH_TOPICS = [
    "import duty tariff changes",
    "anti-dumping duty regulation",
    "customs regulation requirement",
    "product safety compliance standard",
    "packaging labeling requirement",
    "market demand trend outlook",
    "retail back-to-school season",
    "sustainability plastic regulation",
    "trade policy tariff update",
    "raw material price freight logistics",
]

# ── Source type classification ────────────────────────────────────────────────
TRUSTED_DOMAIN_KEYWORDS = {
    "Government":      [".gov", ".gov.", "customs.", "cbp.gov", "usda.gov",
                        "ita.doc.gov", "ama.gov.au", "canada.ca", "gob.mx",
                        "mfat.govt.nz", "gov.uk", "europa.eu", "trade.gov"],
    "Trade Body":      ["wto.org", "worldbank.org", "un.org", "oecd.org",
                        "iata.org", "unctad.org", "imf.org",
                        "icc", "ficci", "assocham"],
    "Reputed News":    ["reuters.com", "bloomberg.com", "ft.com", "wsj.com",
                        "economist.com", "apnews.com", "bbc.com", "nytimes.com",
                        "theguardian.com", "scmp.com", "financialexpress.com",
                        "livemint.com", "businessstandard.com"],
    "Industry Report": ["statista.com", "ibisworld.com", "euromonitor.com",
                        "grandviewresearch.com", "mordorintelligence.com",
                        "marketsandmarkets.com", "businesswire.com",
                        "prnewswire.com", "globenewswire.com"],
    "Retailer Update": ["walmart.com", "amazon.com", "target.com",
                        "officedepot.com", "officeworks.com.au", "staples.com",
                        "costco.com"],
}


# ── DB helpers (self-contained to avoid circular imports with app.py) ─────────
def _get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ── Data loaders ──────────────────────────────────────────────────────────────
def get_active_team_markets():
    """Return {team_name: [market, …]} for all active mappings."""
    with closing(_get_db()) as db:
        rows = db.execute(
            "SELECT team_name, market FROM mi_team_markets "
            "WHERE is_active=1 ORDER BY team_name, sort_order, market"
        ).fetchall()
    result = {}
    for r in rows:
        result.setdefault(r["team_name"], []).append(r["market"])
    return result


def get_active_categories():
    """Return list of active category dicts, each containing their products."""
    with closing(_get_db()) as db:
        cats = db.execute(
            "SELECT * FROM mi_categories WHERE is_active=1 ORDER BY sort_order, id"
        ).fetchall()
        result = []
        for cat in cats:
            cd = dict(cat)
            try:
                cd["keywords"] = json.loads(cd.get("keywords") or "[]")
            except Exception:
                cd["keywords"] = []
            prods = db.execute(
                "SELECT * FROM mi_products WHERE category_id=? AND is_active=1 ORDER BY sort_order, id",
                (cd["id"],)
            ).fetchall()
            pd_list = []
            for p in prods:
                pd = dict(p)
                for f in ("keywords", "alternate_names", "hs_codes"):
                    try:
                        pd[f] = json.loads(pd.get(f) or "[]")
                    except Exception:
                        pd[f] = []
                pd_list.append(pd)
            cd["products"] = pd_list
            result.append(cd)
    return result


# ── Query building ─────────────────────────────────────────────────────────────
def build_queries(market, category, products):
    """Generate Tavily search query strings for one market + category."""
    year = dt.date.today().year
    prod_terms = " ".join(
        [p["name"] for p in products[:4]]
        + [kw for p in products[:2] for kw in p.get("keywords", [])[:2]]
    )
    cat_name = category["name"]
    queries = []
    for topic in SEARCH_TOPICS[:5]:
        queries.append(f"{prod_terms} {topic} {market} {year}")
    queries.append(f"stationery {cat_name} import export regulation {market} {year}")
    return queries


# ── Source helpers ────────────────────────────────────────────────────────────
def classify_source(url, source_name):
    text = (url + " " + source_name).lower()
    for src_type, keywords in TRUSTED_DOMAIN_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            return src_type
    return "Other"


def content_hash(title, url):
    """MD5 of (title+url) for deduplication."""
    return hashlib.md5((str(title) + str(url)).lower().strip().encode()).hexdigest()


# ── Tavily search ─────────────────────────────────────────────────────────────
def search_tavily(query):
    """Run one Tavily search query. Returns list of result dicts."""
    if not TAVILY_API_KEY:
        return []
    try:
        if HAS_TAVILY_SDK:
            client = _TavilyClient(api_key=TAVILY_API_KEY)
            resp = client.search(
                query=query,
                search_depth="advanced",
                max_results=MAX_SEARCH_RESULTS,
                include_answer=False,
                include_raw_content=False,
            )
            results = resp.get("results", [])
        else:
            # Fallback: call Tavily REST API directly
            r = requests.post(
                "https://api.tavily.com/search",
                json={"api_key": TAVILY_API_KEY, "query": query,
                      "search_depth": "advanced", "max_results": MAX_SEARCH_RESULTS},
                timeout=30,
            )
            r.raise_for_status()
            results = r.json().get("results", [])
        logger.info("Tavily '%s…' → %d results", query[:55], len(results))
        return results
    except Exception as exc:
        logger.error("Tavily error: %s", exc)
        return []


# ── AI filter + summarise (NVIDIA primary, OpenRouter fallback) ──────────────
def _chat_completion(prompt):
    """Try NVIDIA NIM first; fall back to OpenRouter on any failure.
    Returns the raw text content, or raises the last error if both fail."""
    last_exc = None
    if NVIDIA_API_KEY:
        try:
            resp = requests.post(
                NVIDIA_URL,
                headers={"Authorization": f"Bearer {NVIDIA_API_KEY}", "Content-Type": "application/json"},
                json={
                    "model": NVIDIA_MODEL,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.2,
                    "top_p": 0.95,
                    "max_tokens": 4096,
                },
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()
            text = data["choices"][0]["message"]["content"].strip()
            if text:
                return text, NVIDIA_MODEL
            logger.warning("NVIDIA returned empty content — falling back to OpenRouter")
        except Exception as exc:
            logger.warning("NVIDIA call failed (%s) — falling back to OpenRouter", exc)
            last_exc = exc

    if not OPENROUTER_API_KEY:
        if last_exc:
            raise last_exc
        raise RuntimeError("Neither NVIDIA_API_KEY nor OPENROUTER_API_KEY is set")

    resp = requests.post(
        OPENROUTER_URL,
        headers={"Authorization": f"Bearer {OPENROUTER_API_KEY}", "Content-Type": "application/json"},
        json={"model": AI_MODEL, "messages": [{"role": "user", "content": prompt}]},
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    data = resp.json()
    return data["choices"][0]["message"]["content"].strip(), AI_MODEL


def ai_filter(market, categories, raw_results, month, year):
    """
    Send Tavily search results to the AI for:
      1. Relevance + credibility filtering
      2. Deduplication
      3. Business-relevant summarisation

    Returns list of structured update dicts ready for DB insertion.
    """
    if not NVIDIA_API_KEY and not OPENROUTER_API_KEY:
        logger.warning("No AI API key set (NVIDIA_API_KEY / OPENROUTER_API_KEY) — skipping AI filter")
        return []
    if not raw_results:
        return []

    cat_lines = []
    for cat in categories:
        pnames = [p["name"] for p in cat.get("products", [])]
        cat_lines.append(f"  • {cat['name']}: {', '.join(pnames) if pnames else '(general)'}")
    cat_context = "\n".join(cat_lines)

    # Trim results to avoid token overrun
    trimmed = []
    for r in raw_results[:40]:
        trimmed.append({
            "title":          (r.get("title") or "")[:200],
            "url":            r.get("url") or "",
            "content":        (r.get("content") or "")[:600],
            "published_date": r.get("published_date") or "",
        })

    period = dt.date(year, month, 1).strftime("%B %Y")

    prompt = f"""You are a senior market intelligence analyst for an Indian stationery export company.

Target market: {market}
Period: {period}
Our product categories and products:
{cat_context}

I am providing {len(trimmed)} web search results. Your tasks:
1. KEEP only results directly relevant to our stationery products and the {market} market.
2. KEEP only results from: government websites, customs authorities, WTO/trade bodies, Reuters, Bloomberg, Financial Times, BBC, AP News, verified industry reports, official retailer announcements.
3. REJECT: blogs without citations, social media, opinion pieces, promotional content, results older than 9 months, off-topic results, duplicates.
4. For near-duplicates (same news, different sources) keep ONLY the most credible source.
5. Write concise business-relevant summaries (2–3 sentences).
6. Assign credibility and relevance scores.

Search results (JSON):
{json.dumps(trimmed, ensure_ascii=False)}

Return a JSON ARRAY ONLY — no markdown fences, no explanation, no other text. Each element:
{{
  "update_title":    "concise factual title, max 130 chars",
  "update_summary":  "2-3 sentences factual, export-business relevant",
  "business_impact": "one sentence — how this affects our export sales / pricing / compliance / demand",
  "category_name":   "exact category name from the list above, or null if cross-category",
  "product_name":    "specific product name if applicable, else null",
  "source_name":     "publication or website name",
  "source_url":      "full URL",
  "source_type":     "Government|Trade Body|Reputed News|Industry Report|Retailer Update|Other",
  "publication_date":"YYYY-MM-DD, or YYYY-MM, or null",
  "credibility_score": <1-10 integer — Government=10, WTO/World Bank=9, Reuters/Bloomberg=8, Industry Report=6, Blog=2>,
  "relevance_score":   <1-10 integer — 10=directly impacts our product+market, 5=moderate, 1=tangential>
}}

Rules:
- Include ONLY if credibility_score ≥ 5 AND relevance_score ≥ 6
- Maximum {MAX_UPDATES_PER_MARKET} updates
- If nothing passes the threshold, return []
- Return the JSON array ONLY"""

    try:
        text, used_model = _chat_completion(prompt)

        # Strip markdown fences if model wraps the JSON
        if text.startswith("```"):
            text = text.split("```", 2)[1]
            if text.startswith("json"):
                text = text[4:]
            text = text.rsplit("```", 1)[0].strip()

        start, end = text.find("["), text.rfind("]") + 1
        if start < 0 or end <= start:
            logger.warning("No JSON array in AI response for %s", market)
            return []

        updates = json.loads(text[start:end])
        logger.info("AI (%s) → %d updates for %s", used_model, len(updates), market)
        return updates

    except requests.exceptions.Timeout:
        logger.error("AI request timed out for %s", market)
        return []
    except (json.JSONDecodeError, KeyError, IndexError) as exc:
        logger.error("AI JSON parse error (%s): %s", market, exc)
        return []
    except Exception as exc:
        logger.error("AI API error (%s): %s", market, exc)
        return []


# ── DB persistence ────────────────────────────────────────────────────────────
def _hash_exists(db, chash, month, year):
    return bool(db.execute(
        "SELECT 1 FROM mi_updates WHERE content_hash=? AND month=? AND year=? LIMIT 1",
        (chash, month, year)
    ).fetchone())


def save_update(db, update, team_name, market, month, year, categories):
    """Insert one update. Returns True if saved, False if duplicate."""
    chash = content_hash(update.get("update_title", ""), update.get("source_url", ""))
    if _hash_exists(db, chash, month, year):
        return False

    cat_id = None
    cat_name = update.get("category_name") or ""
    for cat in categories:
        if cat["name"].lower() == cat_name.lower():
            cat_id = cat["id"]
            break

    now = dt.datetime.now().isoformat(timespec="seconds")
    db.execute(
        """INSERT INTO mi_updates
           (month, year, team_name, market, category_id, category_name, product_name,
            update_title, update_summary, business_impact, source_name, source_url,
            source_type, publication_date, credibility_score, relevance_score,
            content_hash, created_at, updated_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            month, year, team_name, market, cat_id,
            update.get("category_name") or "",
            update.get("product_name") or "",
            update.get("update_title") or "",
            update.get("update_summary") or "",
            update.get("business_impact") or "",
            update.get("source_name") or "",
            update.get("source_url") or "",
            update.get("source_type") or "Other",
            update.get("publication_date") or "",
            float(update.get("credibility_score") or 5),
            float(update.get("relevance_score") or 5),
            chash, now, now,
        )
    )
    return True


# ── Job log helpers ───────────────────────────────────────────────────────────
def _log_start(month, year, job_type="monthly_fetch"):
    now = dt.datetime.now().isoformat(timespec="seconds")
    with closing(_get_db()) as db:
        db.execute(
            "INSERT INTO mi_job_logs (job_type, status, month, year, started_at) VALUES (?,?,?,?,?)",
            (job_type, "running", month, year, now)
        )
        db.commit()
        return db.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]


def _log_done(log_id, fetched, saved, teams, error="", failed=False):
    now = dt.datetime.now().isoformat(timespec="seconds")
    with closing(_get_db()) as db:
        db.execute(
            """UPDATE mi_job_logs
               SET status=?, updates_fetched=?, updates_saved=?,
                   teams_processed=?, error_message=?, completed_at=?
               WHERE id=?""",
            (
                "failed" if failed else "completed",
                fetched, saved,
                json.dumps(teams),
                error, now, log_id,
            )
        )
        db.commit()


# ── Main orchestrator ─────────────────────────────────────────────────────────
def run_intelligence_job(month=None, year=None, teams_filter=None):
    """
    Main monthly intelligence job.
    Flow: Tavily search → OpenRouter AI filter → DB save.

    Args:
        month:        1-12, defaults to current month.
        year:         int, defaults to current year.
        teams_filter: optional list of team names to restrict processing.

    Returns:
        dict — {"ok": bool, "fetched": int, "saved": int, "teams": [...], "error": str}
    """
    today = dt.date.today()
    month = month or today.month
    year  = year  or today.year

    log_id = _log_start(month, year)
    logger.info("MI job starting — %d/%d (log=%d, model=%s, fallback=%s)", month, year, log_id, NVIDIA_MODEL, AI_MODEL)

    teams      = get_active_team_markets()
    categories = get_active_categories()

    if teams_filter:
        teams = {k: v for k, v in teams.items() if k in teams_filter}

    # Preflight checks
    if not teams:
        _log_done(log_id, 0, 0, [], "No active team-market mappings configured", failed=True)
        return {"ok": False, "error": "No active team-market mappings configured"}
    if not categories:
        _log_done(log_id, 0, 0, [], "No active product categories configured", failed=True)
        return {"ok": False, "error": "No active product categories configured"}
    if not TAVILY_API_KEY:
        _log_done(log_id, 0, 0, [], "TAVILY_API_KEY not set", failed=True)
        return {"ok": False, "error": "TAVILY_API_KEY environment variable not set"}
    if not NVIDIA_API_KEY and not OPENROUTER_API_KEY:
        _log_done(log_id, 0, 0, [], "No AI API key set", failed=True)
        return {"ok": False, "error": "Set NVIDIA_API_KEY or OPENROUTER_API_KEY environment variable"}

    total_fetched = 0
    total_saved   = 0
    teams_done    = []

    try:
        with closing(_get_db()) as db:
            for team_name, markets in teams.items():
                for market in markets:
                    logger.info("Processing: team=%s market=%s", team_name, market)

                    # Step 1 — Tavily web search across all categories
                    raw_results = []
                    for cat in categories:
                        queries = build_queries(market, cat, cat.get("products", []))
                        for query in queries:
                            raw_results.extend(search_tavily(query))
                            time.sleep(0.5)   # stay within Tavily rate limit

                    # Deduplicate raw results by URL
                    seen_urls = set()
                    unique_results = []
                    for r in raw_results:
                        url = r.get("url", "")
                        if url and url not in seen_urls:
                            seen_urls.add(url)
                            unique_results.append(r)

                    logger.info("%s/%s: %d unique raw results from Tavily",
                                team_name, market, len(unique_results))

                    # Step 2 — OpenRouter AI filter + summarise
                    updates = ai_filter(market, categories, unique_results, month, year)
                    total_fetched += len(updates)

                    # Step 3 — Save to DB
                    for update in updates:
                        if save_update(db, update, team_name, market, month, year, categories):
                            total_saved += 1
                    db.commit()

                    teams_done.append(f"{team_name}/{market}")
                    time.sleep(2)   # brief pause between markets

        logger.info("MI job done — fetched=%d saved=%d", total_fetched, total_saved)
        _log_done(log_id, total_fetched, total_saved, teams_done)
        return {"ok": True, "fetched": total_fetched, "saved": total_saved, "teams": teams_done}

    except Exception as exc:
        logger.error("MI job error: %s", exc, exc_info=True)
        _log_done(log_id, total_fetched, total_saved, teams_done, str(exc), failed=True)
        return {"ok": False, "error": str(exc)}
