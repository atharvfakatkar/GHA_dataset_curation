"""
GitHub REST API client.
- Uses TokenPool for round-robin authentication.
- Retries transient errors with exponential back-off.
- Parses X-RateLimit-* headers to keep TokenPool in sync.
"""

import base64
import logging
import time
from typing import Optional

import requests

from .token_pool import TokenPool

logger = logging.getLogger(__name__)

BASE = "https://api.github.com"
RETRY_STATUSES = {500, 502, 503, 504}
MAX_RETRIES = 5


class GitHubClient:
    def __init__(self, pool: TokenPool, timeout: int = 30):
        self._pool = pool
        self._timeout = timeout
        self._session = requests.Session()
        self._session.headers.update({
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        })

    # ------------------------------------------------------------------
    # Core HTTP
    # ------------------------------------------------------------------

    def _get(self, url: str, params: dict = None) -> Optional[dict | list]:
        """
        Perform a GET request with token rotation and retry logic.
        Returns parsed JSON on success, None on 404/422/451, raises on others.
        """
        for attempt in range(MAX_RETRIES):
            token = self._pool.acquire()
            self._session.headers["Authorization"] = f"Bearer {token}"

            try:
                resp = self._session.get(url, params=params, timeout=self._timeout)
            except requests.RequestException as exc:
                logger.warning(f"Network error ({attempt+1}/{MAX_RETRIES}): {exc}")
                time.sleep(2 ** attempt)
                continue

            # Update token quota
            remaining = int(resp.headers.get("X-RateLimit-Remaining", 5000))
            reset = float(resp.headers.get("X-RateLimit-Reset", time.time() + 3600))
            self._pool.update(token, remaining, reset)

            if resp.status_code == 200:
                return resp.json()

            if resp.status_code == 403:
                retry_after = int(resp.headers.get("Retry-After", 0))
                if retry_after:
                    reset = time.time() + retry_after
                self._pool.mark_exhausted(token, reset)
                continue  # retry with next token

            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 60))
                self._pool.mark_exhausted(token, time.time() + wait)
                continue

            if resp.status_code in (404, 422, 451):
                logger.debug(f"HTTP {resp.status_code}: {url}")
                return None

            if resp.status_code in RETRY_STATUSES:
                logger.warning(f"HTTP {resp.status_code} on {url} (attempt {attempt+1})")
                time.sleep(2 ** attempt)
                continue

            logger.warning(f"Unexpected HTTP {resp.status_code}: {url}")
            return None

        logger.error(f"Giving up on {url} after {MAX_RETRIES} attempts.")
        return None

    def _paginate(self, url: str, params: dict = None, max_pages: int = 10) -> list:
        """Collect all pages of a list endpoint."""
        params = params or {}
        params.setdefault("per_page", 100)
        results = []
        page = 1
        while page <= max_pages:
            params["page"] = page
            data = self._get(url, params)
            if not data:
                break
            if isinstance(data, dict):
                # Unwrap common list keys
                for key in ("workflow_runs", "workflows", "jobs", "items"):
                    if key in data:
                        data = data[key]
                        break
                else:
                    results.append(data)
                    break
            if not data:
                break
            results.extend(data)
            if len(data) < params["per_page"]:
                break
            page += 1
        return results

    # ------------------------------------------------------------------
    # Public API methods
    # ------------------------------------------------------------------

    def get_repo(self, owner: str, repo: str) -> Optional[dict]:
        return self._get(f"{BASE}/repos/{owner}/{repo}")

    def get_repo_languages(self, owner: str, repo: str) -> dict:
        data = self._get(f"{BASE}/repos/{owner}/{repo}/languages")
        return data if isinstance(data, dict) else {}

    def list_workflow_files(self, owner: str, repo: str) -> list:
        """
        Return all *.yml / *.yaml files from .github/workflows/.
        Each item has at least: name, path, sha, download_url.
        """
        data = self._get(
            f"{BASE}/repos/{owner}/{repo}/contents/.github/workflows"
        )
        if not isinstance(data, list):
            return []
        return [
            f for f in data
            if isinstance(f, dict)
            and f.get("type") == "file"
            and (f.get("name", "").endswith(".yml") or f.get("name", "").endswith(".yaml"))
        ]

    def get_file_content(
        self, owner: str, repo: str, path: str, ref: str = None
    ) -> Optional[str]:
        """Return decoded UTF-8 content of a file, or None."""
        params = {"ref": ref} if ref else {}
        data = self._get(
            f"{BASE}/repos/{owner}/{repo}/contents/{path}", params
        )
        if not isinstance(data, dict):
            return None
        raw = data.get("content", "")
        enc = data.get("encoding", "base64")
        if enc == "base64":
            try:
                return base64.b64decode(raw).decode("utf-8", errors="replace")
            except Exception:
                return None
        return raw or None

    def get_workflow_runs(
        self,
        owner: str,
        repo: str,
        workflow_file: str,
        max_runs: int = 50,
        status: str = "completed",
    ) -> list:
        """
        Fetch up to max_runs completed runs for a specific workflow file.
        workflow_file: e.g. 'ci.yml'
        """
        url = f"{BASE}/repos/{owner}/{repo}/actions/workflows/{workflow_file}/runs"
        params = {"status": status, "per_page": min(max_runs, 100)}
        data = self._get(url, params)
        if not isinstance(data, dict):
            return []
        runs = data.get("workflow_runs", [])
        return runs[:max_runs]

    def get_file_tree(self, owner: str, repo: str, sha: str) -> list:
        """
        Return the recursive file tree for a given commit SHA.
        Returns list of {path, type, sha, size} dicts.
        """
        data = self._get(
            f"{BASE}/repos/{owner}/{repo}/git/trees/{sha}",
            {"recursive": "1"},
        )
        if not isinstance(data, dict):
            return []
        tree = data.get("tree", [])
        # Truncated trees (>100k items) — still return what we got
        if data.get("truncated"):
            logger.debug(f"Tree truncated for {owner}/{repo}@{sha}")
        return [item for item in tree if item.get("type") == "blob"]

    def search_repos(
        self,
        query: str,
        sort: str = "stars",
        order: str = "desc",
        max_results: int = 100,
    ) -> list:
        """Search GitHub for repositories. Returns list of repo full_names."""
        url = f"{BASE}/search/repositories"
        collected = []
        page = 1
        while len(collected) < max_results:
            per_page = min(100, max_results - len(collected))
            data = self._get(url, {
                "q": query,
                "sort": sort,
                "order": order,
                "per_page": per_page,
                "page": page,
            })
            if not isinstance(data, dict):
                break
            items = data.get("items", [])
            if not items:
                break
            collected.extend(items)
            if len(items) < per_page or len(collected) >= data.get("total_count", 0):
                break
            page += 1
            # Search API: max 30 req/min — small courtesy sleep
            time.sleep(2)
        return collected[:max_results]
