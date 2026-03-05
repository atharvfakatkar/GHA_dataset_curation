# GitHub Workflow Dataset Collector

A production-ready Python pipeline that collects GitHub Actions workflow
features from hundreds of open-source repositories and writes them to a CSV
matching the schema of `comprehensive_features.csv`.

---

## Dataset Schema (26 columns)

| Column | Source | Description |
|---|---|---|
| `total_cost_usd` | Run API + YAML | `duration_minutes × runner_cost/min` |
| `duration_minutes` | Run API | `updated_at − created_at` |
| `repo_name` | Run API | `owner/repo` |
| `head_sha` | Run API | Commit SHA that triggered the run |
| `workflow_name` | YAML `name:` | Human-readable workflow name |
| `yaml_line_count` | YAML file | Number of lines in the YAML file |
| `yaml_depth` | YAML file | Maximum YAML nesting depth |
| `job_count` | YAML `jobs:` | Number of jobs defined |
| `total_steps` | YAML `steps:` | Sum of steps across all jobs |
| `avg_steps_per_job` | Computed | `total_steps / job_count` |
| `uses_matrix_strategy` | YAML `strategy.matrix` | Bool |
| `matrix_dimensions` | YAML matrix keys | Count of non-include/exclude keys |
| `matrix_permutations` | YAML matrix values | Product of all value-list lengths |
| `fail_fast` | YAML `strategy.fail-fast` | Bool (default True) |
| `os_label` | YAML `runs-on` | `ubuntu` / `windows` / `macos` / `self-hosted` |
| `container_image` | YAML `container.image` | `False` or image string |
| `timeout_minutes` | YAML `timeout-minutes` | Per-job timeout |
| `unique_actions_used` | YAML `uses:` values | Count of distinct `uses:` references |
| `is_using_setup_actions` | YAML | Bool – any `actions/setup-*` or `actions/checkout` |
| `is_using_docker_actions` | YAML | Bool – any `docker://` action |
| `is_using_cache` | YAML | Bool – any `actions/cache` |
| `env_var_count` | YAML `env:` blocks | Total env vars across all scopes |
| `if_condition_count` | YAML `if:` keys | Total `if:` conditions in the file |
| `needs_dependencies_count` | YAML `needs:` | Total job dependency references |
| `code_complexity` | Lizard (static analysis) | `avg_cyclomatic_complexity / 100` across sampled files |
| `primary_language` | GitHub Repo API | Dominant language of the codebase |

### Runner pricing (GitHub's public rates, used for `total_cost_usd`)
| Runner | Cost/min |
|---|---|
| `ubuntu` | $0.008 |
| `windows` | $0.016 |
| `macos` | $0.080 |
| `self-hosted` | $0.000 |

---

## Architecture

```
github-workflow-dataset/
├── main.py                  ← CLI entry point (argparse)
├── requirements.txt
├── .env.example             ← Copy to .env and add your tokens
├── repos.txt                ← 132 curated repos (edit freely)
└── src/
    ├── config.py            ← All settings from env vars / .env
    ├── token_pool.py        ← Thread-safe round-robin token manager
    ├── github_client.py     ← GitHub REST API wrapper (retry + back-off)
    ├── repo_collector.py    ← Repo discovery (file or GitHub Search)
    ├── yaml_parser.py       ← Extracts all YAML-derived features
    ├── complexity.py        ← lizard-based code complexity per repo
    └── pipeline.py          ← Parallel orchestration + checkpointing
```

---

## How the Pipeline Works

```
┌─────────────────────────────────────────────────────────────┐
│  STEP 1 – Repository Discovery                              │
│  ┌─────────────┐   OR   ┌────────────────────────────────┐ │
│  │  repos.txt  │        │  GitHub Search API             │ │
│  │  (curated)  │        │  (10 language-filtered queries)│ │
│  └─────────────┘        └────────────────────────────────┘ │
└──────────────────────────────┬──────────────────────────────┘
                               │ list of "owner/repo" strings
                               ▼
┌─────────────────────────────────────────────────────────────┐
│  STEP 2 – Parallel Repo Processing  (ThreadPoolExecutor)    │
│                                                             │
│  For each repo (up to MAX_WORKERS threads):                 │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ 2a. GET /repos/{owner}/{repo}                        │  │
│  │     → primary_language, default_branch               │  │
│  │                                                      │  │
│  │ 2b. GET /repos/{owner}/{repo}/branches/{branch}      │  │
│  │     → default_branch_sha (for file tree)             │  │
│  │                                                      │  │
│  │ 2c. Code Complexity  (once per repo)                 │  │
│  │     GET /repos/{}/git/trees/{sha}?recursive=1        │  │
│  │     ├─ filter source files by language extension     │  │
│  │     ├─ random-sample up to 25 files                  │  │
│  │     ├─ GET /repos/{}/contents/{path} (×25)           │  │
│  │     ├─ decode base64 content                         │  │
│  │     └─ lizard.analyze_file() → avg CCN / 100         │  │
│  │                                                      │  │
│  │ 2d. GET /repos/{}/contents/.github/workflows         │  │
│  │     → list of *.yml / *.yaml files                   │  │
│  │                                                      │  │
│  │ 2e. For each workflow YAML file:                     │  │
│  │     ├─ GET /repos/{}/contents/{path}  → YAML text    │  │
│  │     ├─ parse_workflow_yaml()          → 20 features  │  │
│  │     └─ GET /repos/{}/actions/workflows/{file}/runs   │  │
│  │           → up to MAX_RUNS_PER_WORKFLOW completed    │  │
│  │           runs; for each run emit one CSV row        │  │
│  └──────────────────────────────────────────────────────┘  │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│  STEP 3 – Output                                            │
│  • Thread-safe CSV append (one lock, one writer per batch)  │
│  • Checkpoint JSON updated after every repo                 │
│    → restart resumes from where it left off                 │
└─────────────────────────────────────────────────────────────┘
```

### Token Round-Robin Policy

Every API call goes through `TokenPool.acquire()`:

1. Maintain a circular index over the token list.
2. On each `acquire()`, advance the index and check the candidate token's
   remaining quota (updated from `X-RateLimit-Remaining` headers).
3. If the selected token has ≤ 10 remaining calls, skip to the next.
4. If **all** tokens are exhausted, wait until the earliest `X-RateLimit-Reset`
   timestamp, then retry.
5. On HTTP 403/429, immediately mark the token as exhausted and pick another.

With **N tokens** you get ~N × 5 000 = N × 5 000 requests/hour.
Four tokens ≈ 20 000 req/h, enough for ~160 repos/hour.

---

## Setup

### 1 — Prerequisites

- Python 3.10 or newer
- A GitHub account (free)

### 2 — Clone / unzip the project

```bash
cd github-workflow-dataset
```

### 3 — Install dependencies

```bash
pip install -r requirements.txt
```

### 4 — Create GitHub Personal Access Tokens

1. Go to **https://github.com/settings/tokens**
2. Click **"Generate new token (classic)"**
3. Give it a name (e.g. `workflow-collector-1`)
4. **Leave all scopes unchecked** (public data only — no permissions needed)
5. Click **Generate token** — copy it immediately
6. Repeat for 2–4 more tokens for better throughput

### 5 — Configure .env

```bash
cp .env.example .env
```

Open `.env` and paste your tokens:

```env
GITHUB_TOKEN=ghp_AAAA...
GITHUB_TOKEN_2=ghp_BBBB...
GITHUB_TOKEN_3=ghp_CCCC...
```

---

## Running

### Quick start (use the curated `repos.txt`)

```bash
python main.py
```

Output is written incrementally to `output/workflow_features.csv`.
Progress is logged to the console and `output/collector.log`.

### Use GitHub Search instead of repos.txt

```bash
python main.py --repos /dev/null
# Or simply delete repos.txt; the collector falls back to GitHub Search.
```

### Tune for speed

```bash
# 6 parallel workers, 50 runs per workflow, 4 tokens assumed
python main.py --workers 6 --runs 50
```

### Skip code complexity (fastest, ~20 fewer API calls/repo)

```bash
python main.py --skip-complexity
```

### Resume a stopped run

The checkpoint file (`output/checkpoint.json`) tracks finished repos.
Simply re-run `python main.py` — already-done repos are skipped automatically.

### Full option reference

```
python main.py --help

optional arguments:
  --repos FILE            Plain-text repo list (default: repos.txt)
  --output FILE           Output CSV path (default: output/workflow_features.csv)
  --checkpoint FILE       Checkpoint JSON (default: output/checkpoint.json)
  --workers N             Parallel threads (default: 4)
  --runs N                Max runs per workflow file (default: 30)
  --max-repos-per-query N Repos per search query (default: 50)
  --skip-complexity       Skip lizard analysis
  --log-level LEVEL       DEBUG|INFO|WARNING|ERROR (default: INFO)
  --log-file FILE         Log file path (default: output/collector.log)
```

---

## Expected throughput

| Tokens | Workers | Repos/hour | Rows/hour (est.) |
|--------|---------|------------|------------------|
| 1      | 2       | ~40        | ~6 000           |
| 2      | 3       | ~80        | ~12 000          |
| 3      | 4       | ~120       | ~18 000          |
| 4      | 6       | ~160       | ~24 000          |

*Assumes ~5 workflow files/repo × 30 runs = 150 rows/repo average.*
*Complexity analysis adds ~25 API calls/repo.*

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `No GitHub tokens found!` | Ensure `.env` is in the project root and has `GITHUB_TOKEN=…` |
| Many `HTTP 403` in logs | Tokens are rate-limited; add more tokens or reduce `--workers` |
| `No workflow files` warnings | Normal — some repos don't use GitHub Actions |
| Empty `code_complexity` (0.05) | Run without `--skip-complexity`; check that `lizard` installed correctly |
| CSV has duplicate rows | Delete the checkpoint and output, then re-run from scratch |

---

## Extending the dataset

**Add more repos:** Edit `repos.txt` (or let GitHub Search find them).

**Change search queries:** Edit `DEFAULT_SEARCH_QUERIES` in `src/repo_collector.py`.

**Change complexity normalisation:** Edit the formula in `src/complexity.py`
(`normalised = round(avg_ccn / 100.0, 6)`).

**Add new columns:** Add the key to the `COLUMNS` list in `src/pipeline.py`
and populate it inside `process_repo()`.
