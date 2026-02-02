# The Historic Miner (Ground Truth) Component

This component collects actual cost data from GitHub workflow runs to build the target variable for training the cost prediction model.

## Features

- **GitHub API Integration**: Fetches historical workflow run data from multiple repositories
- **Cost Calculation**: Calculates actual costs based on duration and OS-specific rates
- **Rate Limiting**: Implements exponential backoff for GitHub API rate limits
- **Error Handling**: Robust error handling for API failures and missing data
- **Flexible Input**: Accepts repository URLs in multiple formats

## Cost Calculation Formula

```
Actual Cost = Duration_Minutes × OS_Multiplier × Base_Rate
```

**Base Rates:**
- Linux: $0.008 per minute
- Windows: $0.016 per minute  
- macOS: $0.08 per minute

## Setup

1. **Install Dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Set up GitHub Token:**
   Create a `.env` file in the project root:
   ```
   GITHUB_TOKEN=your_github_personal_access_token
   ```
   
   Or export as environment variable:
   ```bash
   export GITHUB_TOKEN=your_github_personal_access_token
   ```

3. **Prepare Repository List:**
   Create a file with repository URLs (one per line):
   ```
   # repos.txt
   https://github.com/microsoft/vscode
   https://github.com/facebook/react
   vuejs/vue
   rust-lang/rust
   ```

## Usage

### Command Line Interface

```bash
# Basic usage
python src/miner.py sample_repos.txt

# Custom output file
python src/miner.py sample_repos.txt -o data/my_run_history.csv

# Fetch last 60 days of data, max 200 runs per repo
python src/miner.py sample_repos.txt -d 60 -m 200
```

### Programmatic Usage

```python
from src.miner import HistoricMiner

# Initialize miner
miner = HistoricMiner()  # Uses GITHUB_TOKEN from environment

# Fetch run history
df = miner.fetch_run_history(
    repo_urls_file='sample_repos.txt',
    output_file='data/raw_run_history.csv',
    days_back=30,
    max_runs_per_repo=100
)

print(f"Collected {len(df)} workflow runs")
print(f"Total cost: ${df['total_cost_usd'].sum():.2f}")
```

## Output Format

The miner generates a CSV file with the following columns:

| Column | Description |
|--------|-------------|
| `repo_name` | Repository name (owner/repo) |
| `run_id` | GitHub workflow run ID |
| `workflow_name` | Name of the workflow |
| `head_branch` | Git branch |
| `head_sha` | Commit SHA |
| `created_at` | Run creation timestamp |
| `updated_at` | Run completion timestamp |
| `duration_minutes` | Total duration in minutes |
| `total_cost_usd` | Calculated cost in USD |
| `job_count` | Number of jobs in the workflow |

## API Rate Limiting

The component implements intelligent rate limiting:
- Monitors remaining API calls
- Automatically waits when rate limit is low
- Uses exponential backoff for failed requests
- Respects GitHub's API limits (5000 calls/hour for authenticated requests)

## Error Handling

- **404 Errors**: Skips repositories that don't exist or aren't accessible
- **403/429 Errors**: Implements exponential backoff and retry logic
- **Malformed Data**: Skips runs with missing or invalid data
- **Network Issues**: Graceful handling of temporary connectivity problems

## Best Practices

1. **Token Permissions**: Ensure your GitHub token has `repo` scope for private repositories
2. **Repository Selection**: Choose repositories with high activity (>50 runs/week) for better data
3. **Time Range**: Use appropriate `days_back` parameter to balance data quantity and API usage
4. **Storage**: Monitor disk space for large datasets
5. **Monitoring**: Check logs for any skipped repositories or API issues

## Integration with Pipeline

This component is the first stage in the 4-stage pipeline:

1. **Historic Miner** → `raw_run_history.csv` (Ground Truth)
2. **Vacuum Extractor** → `comprehensive_features.csv` (Features)
3. **Analyst** → `selected_features.csv` (Selected Features)
4. **Predictor** → `cost_predictor_model.json` (Trained Model)

The output of this component serves as the target variable for training the cost prediction model.
