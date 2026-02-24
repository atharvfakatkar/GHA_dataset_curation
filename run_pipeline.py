#!/usr/bin/env python3
"""
Async Simple script to run the complete pipeline with minimal configuration.
Just provide your repos.txt file and get the comprehensive dataset incredibly fast.
"""

import os
import sys
import argparse
import asyncio
from dotenv import load_dotenv

# Fix for aiohttp on Windows to prevent event loop closure errors
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

try:
    from src.pipeline import IntegratedPipeline
except ModuleNotFoundError as e:
    missing = getattr(e, "name", "")
    if missing in {"github", "PyGithub", "aiohttp"}:
        raise ModuleNotFoundError(
            "Missing dependency. Install dependencies with: python -m pip install -r requirements.txt\n"
            "Make sure you have aiohttp installed."
        ) from e
    raise

def main():
    """Run the async pipeline with default settings."""

    load_dotenv()

    parser = argparse.ArgumentParser(description="Run the Async GitHub Workflow Cost Prediction Pipeline")
    parser.add_argument("repos", nargs="?", help="Path to repos file (one repo per line)")
    parser.add_argument("--repos", dest="repos_file", help="Path to repos file (one repo per line)")
    parser.add_argument("--output", default="data/comprehensive_features.csv", help="Output CSV path")
    parser.add_argument("--days-back", type=int, default=5, help="Number of days to look back for workflow runs")
    parser.add_argument("--max-runs", type=int, default=20, help="Max successful runs per repo")
    parser.add_argument(
        "--github-token",
        default=os.environ.get("GITHUB_TOKEN") or os.environ.get("GITHUB_API_TOKEN"),
        help="GitHub token (defaults to env var GITHUB_TOKEN)",
    )
    args = parser.parse_args()

    repos_file = args.repos_file or args.repos
    if not repos_file:
        print("Usage: python run_pipeline.py <repos.txt>")
        print("Usage: python run_pipeline.py --repos <repos.txt> [--output ... --days-back ... --max-runs ...]")
        return 1
    
    if not os.path.exists(repos_file):
        print(f"Error: Repository file '{repos_file}' not found")
        return 1
    
    print("🚀 Starting Async GitHub Workflow Cost Prediction Pipeline")
    print("=" * 50)
    print(f"Input file: {repos_file}")
    print(f"Output file: {args.output}")
    print(f"Days to look back: {args.days_back}")
    print(f"Max runs per repo: {args.max_runs}")
    print("=" * 50)
    
    try:
        pipeline = IntegratedPipeline(github_token=args.github_token)
        
        # The pipeline handles its own asyncio event loops internally
        df = pipeline.run_pipeline(
            repo_urls_file=repos_file,
            output_file=args.output,
            days_back=args.days_back,
            max_runs_per_repo=args.max_runs
        )
        
        if df.empty:
            print("\n❌ Pipeline failed - no data generated")
            return 1
        
        print("\n🎉 Pipeline completed successfully!")
        print("\n📊 Dataset Summary:")
        print(f"  • Total workflow runs: {len(df)}")
        feature_cols = [col for col in df.columns if col not in ['repo_name', 'run_id', 'workflow_name', 'head_branch', 'head_sha', 'created_at', 'updated_at', 'total_cost_usd']]
        print(f"  • Feature columns: {len(feature_cols)}")
        print(f"  • Total cost: ${df['total_cost_usd'].sum():.2f}")
        print(f"  • Average cost: ${df['total_cost_usd'].mean():.4f}")
        print(f"\n📁 Output file: {args.output}")
        print("✅ Ready for exploratory data analysis!")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        return 1
    finally:
        if 'pipeline' in locals():
            pipeline.cleanup()

if __name__ == "__main__":
    exit(main())