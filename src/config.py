"""
Configuration — all settings loaded from environment variables (or .env file).
"""

import os
import logging


def load_tokens() -> list:
    """
    Load GitHub PATs from environment variables.
    Reads GITHUB_TOKEN, GITHUB_TOKEN_1, GITHUB_TOKEN_2, … GITHUB_TOKEN_N.
    """
    tokens = []
    # Single-token fallback
    single = os.getenv("GITHUB_TOKEN", "").strip()
    if single:
        tokens.append(single)
    # Indexed tokens
    for i in range(1, 21):
        tok = os.getenv(f"GITHUB_TOKEN_{i}", "").strip()
        if tok:
            tokens.append(tok)
    # Deduplicate while preserving order
    seen = set()
    result = []
    for t in tokens:
        if t not in seen:
            seen.add(t)
            result.append(t)
    return result


class Config:
    # Tokens
    TOKENS: list = load_tokens()

    # I/O paths
    OUTPUT_CSV:       str = os.getenv("OUTPUT_CSV",       "output/workflow_features.csv")
    CHECKPOINT_FILE:  str = os.getenv("CHECKPOINT_FILE",  "output/checkpoint.json")
    LOG_FILE:         str = os.getenv("LOG_FILE",         "output/collector.log")
    REPOS_FILE:       str = os.getenv("REPOS_FILE",       "repos.txt")

    # Pipeline knobs
    MAX_WORKERS:           int  = int(os.getenv("MAX_WORKERS",           "4"))
    MAX_RUNS_PER_WORKFLOW: int  = int(os.getenv("MAX_RUNS_PER_WORKFLOW",  "5"))
    MAX_ROWS_PER_REPO:     int  = int(os.getenv("MAX_ROWS_PER_REPO",     "25"))
    MAX_REPOS_PER_QUERY:   int  = int(os.getenv("MAX_REPOS_PER_QUERY",   "50"))
    SKIP_COMPLEXITY:       bool = os.getenv("SKIP_COMPLEXITY", "false").lower() == "true"
    REQUEST_TIMEOUT:       int  = int(os.getenv("REQUEST_TIMEOUT",       "30"))

    # Logging
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")


def setup_logging(cfg: Config):
    os.makedirs(os.path.dirname(cfg.LOG_FILE), exist_ok=True)
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    handlers = [
        logging.StreamHandler(),
        logging.FileHandler(cfg.LOG_FILE, encoding="utf-8"),
    ]
    logging.basicConfig(
        level=getattr(logging, cfg.LOG_LEVEL.upper(), logging.INFO),
        format=fmt,
        handlers=handlers,
    )
