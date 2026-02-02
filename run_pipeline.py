#!/usr/bin/env python3
"""
Simple script to run the complete pipeline with minimal configuration.
Just provide your repos.txt file and get the comprehensive dataset.
"""

import os
import sys
from src.pipeline import IntegratedPipeline

def main():
    """Run the pipeline with default settings."""
    
    # Check if repos file is provided
    if len(sys.argv) != 2:
        print("Usage: python run_pipeline.py <repos.txt>")
        print("\nExample:")
        print("  python run_pipeline.py sample_repos.txt")
        print("\nThis will generate: data/comprehensive_features.csv")
        return 1
    
    repos_file = sys.argv[1]
    
    # Check if file exists
    if not os.path.exists(repos_file):
        print(f"Error: Repository file '{repos_file}' not found")
        return 1
    
    print("🚀 Starting GitHub Workflow Cost Prediction Pipeline")
    print("=" * 50)
    print(f"Input file: {repos_file}")
    print("Output file: data/comprehensive_features.csv")
    print("Days to look back: 30")
    print("Max runs per repo: 50")
    print("=" * 50)
    
    try:
        # Initialize pipeline
        pipeline = IntegratedPipeline()
        
        # Run pipeline
        df = pipeline.run_pipeline(
            repo_urls_file=repos_file,
            output_file='data/comprehensive_features.csv',
            days_back=30,
            max_runs_per_repo=50
        )
        
        if df.empty:
            print("\n❌ Pipeline failed - no data generated")
            return 1
        
        print("\n🎉 Pipeline completed successfully!")
        print("\n📊 Dataset Summary:")
        print(f"  • Total workflow runs: {len(df)}")
        print(f"  • Feature columns: {len([col for col in df.columns if col not in ['repo_name', 'run_id', 'workflow_name', 'head_branch', 'head_sha', 'created_at', 'updated_at']])}")
        print(f"  • Total cost: ${df['total_cost_usd'].sum():.2f}")
        print(f"  • Average cost: ${df['total_cost_usd'].mean():.4f}")
        print(f"\n📁 Output file: data/comprehensive_features.csv")
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
