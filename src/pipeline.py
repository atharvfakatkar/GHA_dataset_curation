"""
Main data-collection pipeline.

For each repository:
  1. Fetch repo metadata (primary language, default branch SHA).
  2. Compute code_complexity once (expensive — cached per repo).
  3. List all GitHub Actions workflow YAML files.
  4. For each YAML file:
       a. Parse YAML → extract static features.
       b. Fetch recent completed workflow runs.
       c. For each run → emit one CSV row.
  5. Append all rows to the output CSV.

Parallel at the *repository* level — each repo is processed by one worker
thread.  Intra-repo calls are sequential (respects GitHub's secondary limits).

Checkpointing: the set of already-processed repos is saved to a JSON file
so a restart resumes from where it left off.
"""

import csv
import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Dict, List, Optional

from .complexity import compute_complexity
from .github_client import GitHubClient
from .yaml_parser import parse_workflow_yaml

logger = logging.getLogger(__name__)

# CSV column order matching the reference dataset
COLUMNS = [
    "total_cost_usd",
    "duration_minutes",
    "repo_name",
    "head_sha",
    "workflow_name",
    "yaml_line_count",
    "yaml_depth",
    "job_count",
    "total_steps",
    "avg_steps_per_job",
    "uses_matrix_strategy",
    "matrix_dimensions",
    "matrix_permutations",
    "fail_fast",
    "os_label",
    "container_image",
    "timeout_minutes",
    "unique_actions_used",
    "is_using_setup_actions",
    "is_using_docker_actions",
    "is_using_cache",
    "env_var_count",
    "if_condition_count",
    "needs_dependencies_count",
    "code_complexity",
    "primary_language",
]

_csv_lock = threading.Lock()
_checkpoint_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _duration_minutes(run: dict) -> float:
    """Compute duration from run timestamps, return minutes."""
    created = run.get("created_at", "")
    updated = run.get("updated_at", "")
    if not created or not updated:
        return 0.0
    try:
        fmt = "%Y-%m-%dT%H:%M:%SZ"
        t0 = datetime.strptime(created, fmt).replace(tzinfo=timezone.utc)
        t1 = datetime.strptime(updated, fmt).replace(tzinfo=timezone.utc)
        delta = (t1 - t0).total_seconds() / 60.0
        return round(max(delta, 0.0), 4)
    except ValueError:
        return 0.0


def _ensure_csv(path: str):
    """Create output CSV with header if it doesn't exist yet."""
    if not os.path.exists(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=COLUMNS)
            writer.writeheader()


def _append_rows(path: str, rows: List[Dict]):
    """Thread-safe CSV append."""
    if not rows:
        return
    with _csv_lock:
        with open(path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
            writer.writerows(rows)


def _load_checkpoint(path: str) -> set:
    if os.path.exists(path):
        try:
            with open(path) as f:
                return set(json.load(f).get("done", []))
        except Exception:
            pass
    return set()


def _save_checkpoint(path: str, done: set):
    with _checkpoint_lock:
        with open(path, "w") as f:
            json.dump({"done": sorted(done), "updated": datetime.utcnow().isoformat()}, f)


# ---------------------------------------------------------------------------
# Per-repo processing
# ---------------------------------------------------------------------------

def process_repo(
    repo_full_name: str,
    client: GitHubClient,
    max_runs_per_workflow: int,
    max_rows_per_repo: int,
    skip_complexity: bool,
) -> List[Dict]:
    """
    Process one repository and return a list of row dicts ready for CSV output.
    """
    owner, repo = repo_full_name.split("/", 1)
    rows = []

    # 1. Repo metadata
    repo_data = client.get_repo(owner, repo)
    if not repo_data:
        logger.warning(f"Skipping {repo_full_name}: repo not found or inaccessible.")
        return []

    primary_language = (repo_data.get("language") or "unknown").lower()
    default_branch = repo_data.get("default_branch", "main")

    # Get default branch SHA (needed for file tree)
    branch_data = client._get(
        f"https://api.github.com/repos/{owner}/{repo}/branches/{default_branch}"
    )
    default_sha = ""
    if isinstance(branch_data, dict):
        default_sha = (
            branch_data.get("commit", {}).get("sha", "")
        )

    # 2. Code complexity (once per repo)
    code_complexity = 0.05
    if not skip_complexity and default_sha:
        try:
            code_complexity = compute_complexity(
                client, owner, repo, default_sha, primary_language
            )
        except Exception as exc:
            logger.warning(f"Complexity failed for {repo_full_name}: {exc}")

    # 3. List workflow YAML files
    workflow_files = client.list_workflow_files(owner, repo)
    if not workflow_files:
        logger.debug(f"No workflow files in {repo_full_name}.")
        return []

    logger.info(
        f"Processing {repo_full_name} — {len(workflow_files)} workflow file(s), "
        f"lang={primary_language}, complexity={code_complexity:.6f}"
    )

    # 4. Per workflow file
    for wf_file in workflow_files:
        # Stop iterating workflow files if per-repo row cap already hit
        if max_rows_per_repo and len(rows) >= max_rows_per_repo:
            break

        wf_path = wf_file.get("path", "")
        wf_filename = wf_file.get("name", "")
        if not wf_filename:
            continue

        # Fetch current YAML
        yaml_content = client.get_file_content(owner, repo, wf_path)
        if not yaml_content:
            logger.debug(f"Could not fetch {wf_path} in {repo_full_name}.")
            continue

        yaml_features = parse_workflow_yaml(yaml_content)
        if not yaml_features:
            logger.debug(f"Could not parse {wf_path}.")
            continue

        cost_per_min = yaml_features.pop("_runner_cost_per_min", 0.008)

        # Fetch recent runs for this workflow file
        runs = client.get_workflow_runs(
            owner, repo, wf_filename, max_runs=max_runs_per_workflow
        )
        logger.debug(
            f"  {wf_filename}: {len(runs)} run(s) fetched."
        )

        for run in runs:
            # Stop early if we've already hit the per-repo row cap
            if max_rows_per_repo and len(rows) >= max_rows_per_repo:
                logger.debug(
                    f"  {repo_full_name}: reached {max_rows_per_repo}-row cap, "
                    f"stopping early."
                )
                break

            duration = _duration_minutes(run)
            cost = round(duration * cost_per_min, 10)
            head_sha = run.get("head_sha", "")

            row = {
                "total_cost_usd": cost,
                "duration_minutes": round(duration, 4),
                "repo_name": repo_full_name,
                "head_sha": head_sha,
                "code_complexity": code_complexity,
                "primary_language": primary_language,
                **yaml_features,
            }
            rows.append(row)

    return rows


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_pipeline(
    repos: List[str],
    client: GitHubClient,
    output_csv: str,
    checkpoint_file: str,
    max_runs_per_workflow: int = 5,
    max_rows_per_repo: int = 25,
    max_workers: int = 4,
    skip_complexity: bool = False,
):
    """
    Main entry-point.  Processes all repos in parallel, writes rows to CSV,
    and checkpoints progress so interrupted runs can be resumed.
    """
    _ensure_csv(output_csv)
    done = _load_checkpoint(checkpoint_file)

    pending = [r for r in repos if r not in done]
    logger.info(
        f"Pipeline start: {len(repos)} total repos, "
        f"{len(done)} already done, {len(pending)} to process."
    )

    total_rows = 0

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(
                process_repo, repo, client,
                max_runs_per_workflow, max_rows_per_repo, skip_complexity
            ): repo
            for repo in pending
        }

        for future in as_completed(futures):
            repo = futures[future]
            try:
                rows = future.result()
                if rows:
                    _append_rows(output_csv, rows)
                    total_rows += len(rows)
                    logger.info(
                        f"✓ {repo}: {len(rows)} rows written "
                        f"(total so far: {total_rows})"
                    )
                else:
                    logger.info(f"✓ {repo}: 0 rows (skipped or empty)")
            except Exception as exc:
                logger.error(f"✗ {repo}: unhandled error — {exc}", exc_info=True)
            finally:
                done.add(repo)
                _save_checkpoint(checkpoint_file, done)

    logger.info(
        f"Pipeline complete. {total_rows} rows written to {output_csv}."
    )
    return total_rows
