"""
Workflow YAML feature extractor.

Parses a GitHub Actions workflow YAML and computes every column that
is derived from the YAML file itself (not from the run API).

Output fields (matching the dataset schema):
  yaml_line_count, yaml_depth, job_count, total_steps, avg_steps_per_job,
  uses_matrix_strategy, matrix_dimensions, matrix_permutations, fail_fast,
  os_label, container_image, timeout_minutes, unique_actions_used,
  is_using_setup_actions, is_using_docker_actions, is_using_cache,
  env_var_count, if_condition_count, needs_dependencies_count, workflow_name
"""

import logging
import math
from typing import Any, Optional

import yaml

logger = logging.getLogger(__name__)

# Runner → cost USD/min (GitHub's public pricing, public repos billed too)
RUNNER_COST = {
    "ubuntu": 0.008,
    "windows": 0.016,
    "macos": 0.08,
    "mac": 0.08,
    "self-hosted": 0.0,
}

SETUP_ACTION_PREFIXES = ("actions/setup-", "actions/checkout")
DOCKER_ACTION_PREFIX = "docker://"
CACHE_ACTION = "actions/cache"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _max_depth(obj: Any, current: int = 0) -> int:
    """Recursively compute YAML nesting depth."""
    if isinstance(obj, dict):
        if not obj:
            return current
        return max(_max_depth(v, current + 1) for v in obj.values())
    if isinstance(obj, list):
        if not obj:
            return current
        return max(_max_depth(v, current) for v in obj)
    return current


def _collect_env_vars(obj: Any) -> int:
    """Count total env: key-value pairs across the entire YAML tree."""
    count = 0
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "env" and isinstance(v, dict):
                count += len(v)
            else:
                count += _collect_env_vars(v)
    elif isinstance(obj, list):
        for item in obj:
            count += _collect_env_vars(item)
    return count


def _collect_if_conditions(obj: Any) -> int:
    """Count all `if:` keys in the YAML tree."""
    count = 0
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "if":
                count += 1
            count += _collect_if_conditions(v)
    elif isinstance(obj, list):
        for item in obj:
            count += _collect_if_conditions(item)
    return count


def _collect_uses(obj: Any) -> list:
    """Collect all `uses:` values (action references) in the YAML tree."""
    found = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "uses" and isinstance(v, str):
                found.append(v.strip())
            else:
                found.extend(_collect_uses(v))
    elif isinstance(obj, list):
        for item in obj:
            found.extend(_collect_uses(item))
    return found


def _normalize_runner(runs_on: Any) -> str:
    """
    Simplify a runs-on value to one of: ubuntu / windows / macos / self-hosted.
    Accepts strings, lists (matrix), or dicts.
    """
    if isinstance(runs_on, list):
        runs_on = runs_on[0] if runs_on else "ubuntu-latest"
    if isinstance(runs_on, dict):
        # Self-hosted group/labels dict
        return "self-hosted"
    label = str(runs_on).lower()
    if "self-hosted" in label:
        return "self-hosted"
    if "windows" in label:
        return "windows"
    if "mac" in label:
        return "macos"
    return "ubuntu"


def _runner_cost_per_min(os_label: str) -> float:
    return RUNNER_COST.get(os_label, 0.008)


def _matrix_permutations(matrix_cfg: Any) -> tuple:
    """
    Returns (dimensions, permutations) from a strategy.matrix block.
    Skips 'include' / 'exclude' special keys.
    """
    if not isinstance(matrix_cfg, dict):
        return 0, 1
    dims = 0
    perms = 1
    for k, v in matrix_cfg.items():
        if k in ("include", "exclude"):
            continue
        if isinstance(v, list) and v:
            dims += 1
            perms *= len(v)
    return dims, max(perms, 1)


# ---------------------------------------------------------------------------
# Main parser
# ---------------------------------------------------------------------------

def parse_workflow_yaml(content: str) -> Optional[dict]:
    """
    Parse a raw YAML string and return a dict of extracted features.
    Returns None if the content cannot be parsed.
    """
    try:
        doc = yaml.safe_load(content)
    except yaml.YAMLError as exc:
        logger.debug(f"YAML parse error: {exc}")
        return None

    if not isinstance(doc, dict):
        return None

    lines = content.splitlines()
    yaml_line_count = len(lines)
    yaml_depth = _max_depth(doc)
    workflow_name = doc.get("name", "unknown")

    # ---- Jobs ----
    jobs = doc.get("jobs", {})
    if not isinstance(jobs, dict):
        jobs = {}

    job_count = len(jobs)
    total_steps = 0
    uses_matrix_strategy = False
    matrix_dimensions = 0
    matrix_permutations_val = 1
    fail_fast = True            # GitHub default
    os_label = "ubuntu"
    container_image = False
    timeout_minutes = 0
    needs_count = 0

    for job_name, job_cfg in jobs.items():
        if not isinstance(job_cfg, dict):
            continue

        # Steps
        steps = job_cfg.get("steps", [])
        if isinstance(steps, list):
            total_steps += len(steps)

        # Matrix
        strategy = job_cfg.get("strategy", {})
        if isinstance(strategy, dict) and "matrix" in strategy:
            uses_matrix_strategy = True
            dims, perms = _matrix_permutations(strategy["matrix"])
            matrix_dimensions = max(matrix_dimensions, dims)
            matrix_permutations_val = max(matrix_permutations_val, perms)
            # fail-fast default is True in GH Actions
            fail_fast = strategy.get("fail-fast", True)

        # Runner
        runs_on = job_cfg.get("runs-on", "ubuntu-latest")
        os_label = _normalize_runner(runs_on)

        # Container
        container = job_cfg.get("container")
        if container:
            if isinstance(container, str):
                container_image = container
            elif isinstance(container, dict):
                container_image = container.get("image", True)
            else:
                container_image = True
        
        # Timeout
        to = job_cfg.get("timeout-minutes", 0)
        try:
            timeout_minutes = max(timeout_minutes, int(to))
        except (TypeError, ValueError):
            pass

        # Needs
        needs = job_cfg.get("needs", [])
        if isinstance(needs, str):
            needs = [needs]
        if isinstance(needs, list):
            needs_count += len(needs)

    avg_steps_per_job = round(total_steps / job_count, 1) if job_count else 0.0

    # ---- Actions usage ----
    all_uses = _collect_uses(doc)
    unique_actions = len(set(all_uses))
    is_using_setup = any(u.startswith(SETUP_ACTION_PREFIXES) for u in all_uses)
    is_using_docker = any(u.startswith(DOCKER_ACTION_PREFIX) for u in all_uses)
    is_using_cache = any(u.startswith(CACHE_ACTION) for u in all_uses)

    # ---- Env vars & conditions ----
    env_var_count = _collect_env_vars(doc)
    if_condition_count = _collect_if_conditions(doc)

    return {
        "workflow_name": workflow_name,
        "yaml_line_count": yaml_line_count,
        "yaml_depth": yaml_depth,
        "job_count": job_count,
        "total_steps": total_steps,
        "avg_steps_per_job": avg_steps_per_job,
        "uses_matrix_strategy": uses_matrix_strategy,
        "matrix_dimensions": matrix_dimensions,
        "matrix_permutations": matrix_permutations_val,
        "fail_fast": fail_fast,
        "os_label": os_label,
        "container_image": container_image,
        "timeout_minutes": timeout_minutes,
        "unique_actions_used": unique_actions,
        "is_using_setup_actions": is_using_setup,
        "is_using_docker_actions": is_using_docker,
        "is_using_cache": is_using_cache,
        "env_var_count": env_var_count,
        "if_condition_count": if_condition_count,
        "needs_dependencies_count": needs_count,
        # helper for cost calculation
        "_runner_cost_per_min": _runner_cost_per_min(os_label),
    }
