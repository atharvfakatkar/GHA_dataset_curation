#!/usr/bin/env python3
"""
GitHub Workflow Dataset Collector
==================================
Collects GitHub Actions workflow features from public open-source repositories
and writes them to a CSV matching the schema of comprehensive_features.csv.

Usage
-----
  # Load tokens from .env, discover repos via GitHub Search
  python main.py

  # Use a custom list of repos (one owner/repo per line)
  python main.py --repos repos.txt

  # Skip the expensive code-complexity step (faster, sets complexity=0.05)
  python main.py --skip-complexity

  # Tune parallelism and run count
  python main.py --workers 6 --runs 50

Run `python main.py --help` for the full option list.
"""

import argparse
import logging
import os
import sys

# Load .env file if python-dotenv is installed
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from src.config import Config, setup_logging
from src.token_pool import TokenPool
from src.github_client import GitHubClient
from src.repo_collector import get_repos
from src.pipeline import run_pipeline

logger = logging.getLogger(__name__)


def parse_args():
    p = argparse.ArgumentParser(
        description="Collect GitHub Actions workflow features into a CSV dataset.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--repos", metavar="FILE",
        default=None,
        help="Plain-text file with one 'owner/repo' per line. "
             "If omitted, GitHub Search is used for discovery.",
    )
    p.add_argument(
        "--output", metavar="FILE",
        default=Config.OUTPUT_CSV,
        help="Output CSV file path.",
    )
    p.add_argument(
        "--checkpoint", metavar="FILE",
        default=Config.CHECKPOINT_FILE,
        help="JSON checkpoint file (resume support).",
    )
    p.add_argument(
        "--workers", type=int,
        default=Config.MAX_WORKERS,
        help="Number of parallel repo-processing threads.",
    )
    p.add_argument(
        "--runs", type=int,
        default=Config.MAX_RUNS_PER_WORKFLOW,
        help="Max workflow runs to collect per workflow file (per-workflow limit).",
    )
    p.add_argument(
        "--max-rows-per-repo", type=int,
        default=Config.MAX_ROWS_PER_REPO,
        help="Hard cap on total rows emitted per repo across all its workflows. "
             "Set to 0 to disable. Prevents one repo with many workflows from "
             "dominating the dataset.",
    )
    p.add_argument(
        "--max-repos-per-query", type=int,
        default=Config.MAX_REPOS_PER_QUERY,
        help="Max repos fetched per search query (when using discovery).",
    )
    p.add_argument(
        "--skip-complexity", action="store_true",
        default=Config.SKIP_COMPLEXITY,
        help="Skip code-complexity analysis (saves ~20 API calls per repo).",
    )
    p.add_argument(
        "--log-level",
        default=Config.LOG_LEVEL,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log verbosity.",
    )
    p.add_argument(
        "--log-file",
        default=Config.LOG_FILE,
        help="Path to log file.",
    )
    return p.parse_args()


def main():
    args = parse_args()

    # Apply CLI overrides to config
    Config.LOG_LEVEL = args.log_level
    Config.LOG_FILE  = args.log_file
    setup_logging(Config)

    logger.info("=" * 60)
    logger.info("GitHub Workflow Dataset Collector")
    logger.info("=" * 60)

    # Validate tokens
    tokens = Config.TOKENS
    if not tokens:
        logger.error(
            "No GitHub tokens found!\n"
            "Set GITHUB_TOKEN (or GITHUB_TOKEN_1 … GITHUB_TOKEN_N) in your "
            ".env file or environment.\n"
            "Create free tokens at: https://github.com/settings/tokens\n"
            "No special scopes are needed for public repositories."
        )
        sys.exit(1)

    logger.info(f"Token pool: {len(tokens)} token(s) loaded (round-robin).")

    # Initialise client
    pool   = TokenPool(tokens)
    client = GitHubClient(pool, timeout=Config.REQUEST_TIMEOUT)

    # Resolve repos
    repos_file = args.repos or Config.REPOS_FILE
    repos = get_repos(
        client,
        repos_file=repos_file,
        max_per_query=args.max_repos_per_query,
    )

    if not repos:
        logger.error("No repositories to process. Exiting.")
        sys.exit(1)

    logger.info(f"Repos to process: {len(repos)}")
    logger.info(f"Output CSV      : {args.output}")
    logger.info(f"Checkpoint      : {args.checkpoint}")
    logger.info(f"Workers         : {args.workers}")
    logger.info(f"Max runs/wf     : {args.runs}  ← per workflow file")
    logger.info(f"Max rows/repo   : {args.max_rows_per_repo}  ← hard cap across all workflows in one repo")
    logger.info(f"Skip complexity : {args.skip_complexity}")
    logger.info("-" * 60)

    # Run
    total = run_pipeline(
        repos=repos,
        client=client,
        output_csv=args.output,
        checkpoint_file=args.checkpoint,
        max_runs_per_workflow=args.runs,
        max_rows_per_repo=args.max_rows_per_repo,
        max_workers=args.workers,
        skip_complexity=args.skip_complexity,
    )

    logger.info(f"Done. {total} rows collected → {args.output}")


if __name__ == "__main__":
    main()
