import os
from pathlib import Path


def _parse_env_line(line: str) -> tuple[str, str] | None:
    line = line.strip()
    if not line or line.startswith("#") or "=" not in line:
        return None
    key, value = line.split("=", 1)
    key = key.strip()
    value = value.strip()
    if not key:
        return None
    if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
        value = value[1:-1]
    return key, value


def load_env_file(start_path: str | None = None) -> None:
    base = Path(start_path or __file__).resolve()
    search_dirs = [base.parent] + list(base.parents)
    for directory in search_dirs:
        env_path = directory / ".env"
        if not env_path.is_file():
            continue
        try:
            with env_path.open("r", encoding="utf-8") as fh:
                for raw_line in fh:
                    parsed = _parse_env_line(raw_line)
                    if not parsed:
                        continue
                    key, value = parsed
                    os.environ.setdefault(key, value)
        except OSError:
            pass
        return
