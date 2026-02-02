# GitHub Workflow Cost Prediction Pipeline

This integrated pipeline combines **The Historic Miner** and **The Vacuum Extractor** to generate a comprehensive dataset for exploratory data analysis and machine learning model training.

## 🚀 Quick Start

### 1. Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Set up GitHub token
echo "GITHUB_TOKEN=your_github_personal_access_token" > .env
```

### 2. Run the Pipeline

```bash
# Simple usage - just provide your repos file
python run_pipeline.py sample_repos.txt

# Or use the advanced pipeline with more options
python src/pipeline.py sample_repos.txt -o data/my_dataset.csv -d 60 -m 100
```

### 3. Output

The pipeline generates `data/comprehensive_features.csv` containing:
- **Target Variable**: Actual cost data (`total_cost_usd`)
- **25+ Features**: Comprehensive feature set across 5 categories

## 📊 Dataset Structure

### Target Variables
| Column | Description |
|--------|-------------|
| `total_cost_usd` | **Primary target** - Actual workflow cost in USD |
| `duration_minutes` | Total execution time |
| `job_count` | Number of jobs in workflow |

### Feature Categories

#### 🏗️ Category 1: Structural Complexity
- `yaml_line_count`: Raw length of workflow file
- `yaml_depth`: Maximum nesting level
- `total_steps`: Sum of steps across all jobs
- `avg_steps_per_job`: Average steps per job

#### 🔄 Category 2: Matrix Multipliers
- `uses_matrix_strategy`: Boolean - uses matrix builds
- `matrix_dimensions`: Number of matrix variables
- `matrix_permutations`: Expansion factor (mathematical product)
- `fail_fast`: Boolean - fail-fast behavior

#### 🖥️ Category 3: Execution Environment
- `os_label`: Operating system (ubuntu/windows/macos)
- `container_image`: Boolean - uses Docker containers
- `timeout_minutes`: Average timeout across jobs

#### ⚡ Category 4: Action Ecosystem
- `unique_actions_used`: Count of distinct GitHub Actions
- `is_using_setup_actions`: Boolean - uses setup actions
- `is_using_docker_actions`: Boolean - uses Docker actions
- `is_using_cache`: Boolean - uses caching

#### 🔀 Category 5: Logic & Flow
- `env_var_count`: Number of environment variables
- `if_condition_count`: Number of conditional statements
- `needs_dependencies_count`: Job dependencies count

## 🔧 Configuration Options

### Command Line Arguments

```bash
python src/pipeline.py <repos_file> [options]

Arguments:
  repos_file              File containing GitHub repository URLs

Options:
  -o, --output FILE       Output CSV file (default: data/comprehensive_features.csv)
  -d, --days INT          Number of days to look back (default: 30)
  -m, --max-runs INT      Max runs per repository (default: 50)
```

### Repository File Format

Create a text file with repository URLs (one per line):

```
# repos.txt
https://github.com/microsoft/vscode
https://github.com/facebook/react
vuejs/vue
rust-lang/rust
```

## 📈 Pipeline Stages

### Stage 1: Historic Mining
- ✅ Fetches workflow runs from GitHub API
- ✅ Filters for successful runs only
- ✅ Calculates actual costs using OS-specific rates
- ✅ Handles rate limiting and errors gracefully

### Stage 2: Feature Extraction
- ✅ Time-travel to specific commit SHA
- ✅ Discovers workflow files automatically
- ✅ Extracts 25+ comprehensive features
- ✅ Handles malformed YAML gracefully

### Stage 3: Integration
- ✅ Combines cost data with extracted features
- ✅ Matches workflow runs to YAML configurations
- ✅ Generates unified dataset for analysis

## 🎯 Usage Examples

### Basic Research Dataset
```bash
# Generate dataset for EDA
python run_pipeline.py research_repos.txt
```

### Large Scale Analysis
```bash
# Process more data for better model training
python src/pipeline.py large_repos.txt -d 90 -m 200
```

### Custom Output Location
```bash
# Save to specific location
python src/pipeline.py sample_repos.txt -o experiments/dataset_v1.csv
```

## 📊 Output Analysis

Once you have the dataset, you can perform EDA:

```python
import pandas as pd

# Load the comprehensive dataset
df = pd.read_csv('data/comprehensive_features.csv')

# Basic statistics
print(f"Total runs: {len(df)}")
print(f"Total cost: ${df['total_cost_usd'].sum():.2f}")
print(f"Average cost: ${df['total_cost_usd'].mean():.4f}")

# Feature correlation with cost
correlations = df.corr()['total_cost_usd'].sort_values(ascending=False)
print("\nTop features correlated with cost:")
print(correlations.head(10))
```

## 🔍 Advanced Usage

### Custom Feature Analysis
```python
# Analyze matrix strategy impact
matrix_runs = df[df['uses_matrix_strategy'] == True]
non_matrix_runs = df[df['uses_matrix_strategy'] == False]

print(f"Matrix strategy avg cost: ${matrix_runs['total_cost_usd'].mean():.4f}")
print(f"Non-matrix avg cost: ${non_matrix_runs['total_cost_usd'].mean():.4f}")
```

### OS-based Cost Comparison
```python
# Compare costs across operating systems
os_costs = df.groupby('os_label')['total_cost_usd'].agg(['mean', 'count', 'sum'])
print(os_costs.sort_values('mean', ascending=False))
```

## 🚨 Important Notes

### GitHub Token Requirements
- **Required**: Personal Access Token with `repo` scope
- **Rate Limits**: 5000 requests/hour for authenticated requests
- **Private Repos**: Token needs access to private repositories

### Performance Considerations
- **API Rate Limits**: Pipeline implements intelligent rate limiting
- **Processing Time**: Depends on repository size and activity
- **Disk Space**: Ensure sufficient space for cloned repositories

### Data Quality
- **Successful Runs Only**: Filters out failed/incomplete runs
- **Feature Extraction**: Skips malformed YAML files
- **Cost Accuracy**: Based on official GitHub Actions pricing

## 🔧 Troubleshooting

### Common Issues

1. **"GitHub token is required"**
   - Set `GITHUB_TOKEN` environment variable
   - Create `.env` file with your token

2. **"Repository not found"**
   - Check repository URLs in input file
   - Verify token has access to repositories

3. **"No workflow files found"**
   - Repository might not use GitHub Actions
   - Workflow files might be in different location

4. **Rate Limit Errors**
   - Pipeline handles automatically with exponential backoff
   - Consider reducing `max_runs_per_repo` for large datasets

### Debug Mode

Enable detailed logging:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## 📚 Next Steps

After generating the dataset:

1. **Exploratory Data Analysis**: Use the dataset for correlation analysis
2. **Feature Selection**: Apply the Analyst component to select top features
3. **Model Training**: Train XGBoost model on selected features
4. **Validation**: Compare predictions against actual costs

## 📄 Files Generated

- `data/comprehensive_features.csv` - Main dataset (25+ features + cost data)
- `data/temp_raw_run_history.csv` - Temporary raw cost data
- Logs and progress reports in console output

## 🎉 Success Criteria

When the pipeline completes successfully, you should see:
- ✅ Dataset with 1000+ workflow runs (depending on input)
- ✅ 25+ feature columns extracted
- ✅ Cost data successfully calculated and integrated
- ✅ Ready for exploratory data analysis!
