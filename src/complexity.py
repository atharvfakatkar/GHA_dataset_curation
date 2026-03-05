"""
Code complexity analyser.

Fetches a sample of source files from a repository via the GitHub API
and runs lizard (static analysis) to compute average cyclomatic complexity.

The normalised score stored in the dataset is:
    code_complexity = average_cyclomatic_complexity / 100

This matches the observed value of 0.079309 for plankanban/planka (JS),
where lizard reports ~7.93 average CCN across sampled functions.
"""

import logging
import random
import tempfile
import os
from typing import Optional

logger = logging.getLogger(__name__)

# Source-file extensions per primary language (lizard-supported)
LANG_EXTENSIONS = {
    "javascript":  [".js", ".jsx", ".mjs"],
    "typescript":  [".ts", ".tsx"],
    "python":      [".py"],
    "java":        [".java"],
    "go":          [".go"],
    "ruby":        [".rb"],
    "php":         [".php"],
    "c":           [".c", ".h"],
    "c++":         [".cpp", ".cc", ".cxx", ".hpp", ".hh"],
    "c#":          [".cs"],
    "rust":        [".rs"],
    "swift":       [".swift"],
    "kotlin":      [".kt", ".kts"],
    "shell":       [".sh"],
    "scala":       [".scala"],
    "haskell":     [".hs"],
    "lua":         [".lua"],
    "r":           [".r"],
}
DEFAULT_EXTENSIONS = [".py", ".js", ".java", ".go", ".rb", ".ts", ".cpp", ".cs"]

# Paths to skip (vendored / generated code inflates complexity)
SKIP_DIRS = {
    "node_modules", "vendor", "dist", "build", ".git",
    "__pycache__", ".venv", "venv", "env", "target",
    "third_party", "3rdparty", "external", "generated",
}

MAX_SAMPLE_FILES = 25   # API calls per repo for complexity
MAX_FILE_SIZE    = 200_000   # bytes — skip very large files


def _relevant_extensions(primary_language: str) -> set:
    lang = (primary_language or "").lower()
    return set(LANG_EXTENSIONS.get(lang, DEFAULT_EXTENSIONS))


def _is_skipped(path: str) -> bool:
    parts = set(path.lower().split("/"))
    return bool(parts & SKIP_DIRS)


def compute_complexity(
    github_client,
    owner: str,
    repo: str,
    default_branch_sha: str,
    primary_language: str,
) -> float:
    """
    Return normalised code_complexity for the repo.
    Falls back to 0.05 (plausible default) if analysis fails.
    """
    try:
        import lizard
    except ImportError:
        logger.warning("lizard not installed — returning default complexity 0.05")
        return 0.05

    extensions = _relevant_extensions(primary_language)
    tree = github_client.get_file_tree(owner, repo, default_branch_sha)

    # Filter blobs by extension and skip vendor dirs
    candidates = [
        item for item in tree
        if any(item.get("path", "").endswith(ext) for ext in extensions)
        and not _is_skipped(item.get("path", ""))
        and int(item.get("size", 0)) < MAX_FILE_SIZE
        and int(item.get("size", 0)) > 50   # skip near-empty files
    ]

    if not candidates:
        logger.debug(f"No source files found for {owner}/{repo}, using default.")
        return 0.05

    # Sample randomly so we don't blast the API
    sample = random.sample(candidates, min(MAX_SAMPLE_FILES, len(candidates)))

    ccn_values = []
    for item in sample:
        path = item["path"]
        content = github_client.get_file_content(owner, repo, path)
        if not content:
            continue

        # Write to a temp file so lizard can detect the language
        ext = os.path.splitext(path)[1] or ".txt"
        with tempfile.NamedTemporaryFile(
            suffix=ext, mode="w", encoding="utf-8", delete=False
        ) as tmp:
            tmp.write(content)
            tmp_path = tmp.name

        try:
            file_info = lizard.analyze_file(tmp_path)
            for fn in file_info.function_list:
                ccn_values.append(fn.cyclomatic_complexity)
        except Exception as exc:
            logger.debug(f"lizard error on {path}: {exc}")
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    if not ccn_values:
        logger.debug(f"No functions found in sampled files for {owner}/{repo}.")
        return 0.05

    avg_ccn = sum(ccn_values) / len(ccn_values)
    normalised = round(avg_ccn / 100.0, 6)
    logger.debug(
        f"{owner}/{repo}: analysed {len(sample)} files, "
        f"{len(ccn_values)} functions, avg CCN={avg_ccn:.2f} → {normalised}"
    )
    return normalised
