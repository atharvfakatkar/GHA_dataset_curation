"""
Repository discovery.

Two modes:
  1. File-based  — read a plain-text file with one "owner/repo" per line.
  2. Search-based — query the GitHub Search API for repos that have
                    workflow files, filtering by language/stars/size.

Deduplicates results before returning.
"""

import logging
import os
from typing import List

logger = logging.getLogger(__name__)

# Searches that yield repos known to have non-trivial CI pipelines
DEFAULT_SEARCH_QUERIES = [
    'path:.github/workflows language:javascript stars:>200 pushed:>2023-01-01',
    'path:.github/workflows language:python stars:>200 pushed:>2023-01-01',
    'path:.github/workflows language:go stars:>200 pushed:>2023-01-01',
    'path:.github/workflows language:java stars:>200 pushed:>2023-01-01',
    'path:.github/workflows language:typescript stars:>200 pushed:>2023-01-01',
    'path:.github/workflows language:rust stars:>100 pushed:>2023-01-01',
    'path:.github/workflows language:ruby stars:>100 pushed:>2023-01-01',
    'path:.github/workflows language:php stars:>100 pushed:>2023-01-01',
    'path:.github/workflows language:c++ stars:>100 pushed:>2023-01-01',
    'path:.github/workflows language:kotlin stars:>100 pushed:>2023-01-01',
]


def load_repos_from_file(path: str) -> List[str]:
    """
    Read repos from a plain-text file (one 'owner/repo' per line).
    Lines starting with '#' are treated as comments and skipped.
    """
    repos = []
    with open(path, "r") as fh:
        for line in fh:
            line = line.strip()
            if line and not line.startswith("#"):
                repos.append(line)
    logger.info(f"Loaded {len(repos)} repo(s) from {path}.")
    return repos


def discover_repos(
    github_client,
    queries: List[str] = None,
    max_per_query: int = 50,
) -> List[str]:
    """
    Run search queries against GitHub and return a deduplicated list of
    'owner/repo' strings.
    """
    queries = queries or DEFAULT_SEARCH_QUERIES
    seen = set()
    result = []

    for q in queries:
        logger.info(f"Searching: {q!r} (up to {max_per_query} results)")
        items = github_client.search_repos(q, max_results=max_per_query)
        for item in items:
            full_name = item.get("full_name", "")
            if full_name and full_name not in seen:
                seen.add(full_name)
                result.append(full_name)
        logger.info(f"  → {len(result)} unique repos so far.")

    return result


def get_repos(
    github_client,
    repos_file: str = None,
    search_queries: List[str] = None,
    max_per_query: int = 50,
) -> List[str]:
    """
    Primary entry point.
    If repos_file exists, load from file; otherwise run discovery search.
    """
    if repos_file and os.path.isfile(repos_file):
        return load_repos_from_file(repos_file)
    
    logger.info("No repos file found — running GitHub search discovery.")
    return discover_repos(github_client, search_queries, max_per_query)
