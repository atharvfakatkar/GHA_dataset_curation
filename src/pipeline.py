#!/usr/bin/env python3
"""
Integrated Pipeline: Historic Miner + Vacuum Extractor
Combines cost data collection with comprehensive feature extraction to generate the unfiltered dataset.
"""

import os
import logging
import pandas as pd
from typing import List, Dict, Optional, Tuple
from pathlib import Path
import time
import json

from src.miner import HistoricMiner
from src.extractor import VacuumExtractor

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class IntegratedPipeline:
    """
    Integrated pipeline that combines historic mining with feature extraction
    to generate the comprehensive dataset for EDA.
    """
    
    def __init__(self, github_token: Optional[str] = None):
        """
        Initialize the pipeline with both components.
        
        Args:
            github_token: GitHub personal access token
        """
        self.miner = HistoricMiner(github_token)
        self.extractor = VacuumExtractor()
        self._repo_cache = {}  # Cache for cloned repositories
        logger.info("Initialized integrated pipeline with miner and extractor")
    
    def _get_cached_repo_path(self, repo_url: str, commit_sha: str) -> str:
        """
        Get cached repository path or clone if not cached.
        Uses repo URL only for caching (not commit-specific) to avoid re-cloning.
        
        Args:
            repo_url: Repository URL
            commit_sha: Commit hash
            
        Returns:
            Path to cloned repository
        """
        # Cache by repository URL only, not by commit, to avoid re-cloning
        cache_key = repo_url
        
        if cache_key not in self._repo_cache:
            repo_path = self.extractor._clone_repo_to_commit(repo_url, commit_sha)
            self._repo_cache[cache_key] = repo_path
            logger.info(f"Cached repository: {cache_key}")
        else:
            # Repository already cached, just checkout to the desired commit
            repo_path = self._repo_cache[cache_key]
            logger.info(f"Using cached repository: {cache_key}")
            
            # Checkout to the specific commit
            try:
                import subprocess
                subprocess.run([
                    'git', 'checkout', commit_sha
                ], check=True, capture_output=True, text=True, cwd=repo_path)
                logger.info(f"Checked out cached repo to commit {commit_sha}")
            except subprocess.CalledProcessError:
                # If checkout fails, use fallback (already handled in _clone_repo_to_commit)
                logger.warning(f"Could not checkout {commit_sha}, using current state")
        
        return self._repo_cache[cache_key]
    
    def _discover_workflow_files_cached(self, repo_url: str, commit_sha: str) -> List[str]:
        """
        Discover workflow files using cached repository.
        
        Args:
            repo_url: GitHub repository URL
            commit_sha: Commit hash
            
        Returns:
            List of workflow file paths
        """
        try:
            # Get cached repository path
            repo_path = self._get_cached_repo_path(repo_url, commit_sha)
            
            # Look for workflow files
            workflows_dir = os.path.join(repo_path, '.github', 'workflows')
            workflow_files = []
            
            if os.path.exists(workflows_dir):
                for file_name in os.listdir(workflows_dir):
                    if file_name.endswith('.yml') or file_name.endswith('.yaml'):
                        workflow_files.append(os.path.join('.github', 'workflows', file_name))
            
            logger.info(f"Found {len(workflow_files)} workflow files in {repo_url}")
            return workflow_files
            
        except Exception as e:
            logger.error(f"Failed to discover workflow files in {repo_url}: {e}")
            return []
    
    def _extract_features_for_run(self, repo_url: str, commit_sha: str, 
                                workflow_name: str) -> Optional[Dict]:
        """
        Extract features for a specific workflow run using cached repository.
        
        Args:
            repo_url: Repository URL
            commit_sha: Commit hash
            workflow_name: Workflow name
            
        Returns:
            Extracted features or None
        """
        try:
            # Get cached repository path
            repo_path = self._get_cached_repo_path(repo_url, commit_sha)
            
            # Discover workflow files
            workflow_files = self._discover_workflow_files_cached(repo_url, commit_sha)
            
            for workflow_file in workflow_files:
                # Read workflow file directly from cached repo
                workflow_path = os.path.join(repo_path, workflow_file)
                
                if os.path.exists(workflow_path):
                    with open(workflow_path, 'r', encoding='utf-8') as f:
                        yaml_content = f.read()
                    
                    # Extract features
                    features = self.extractor.parse_yaml_to_vector(yaml_content)
                    
                    if features:
                        # Match workflow name (strip .yml/.yaml extension)
                        file_name = os.path.basename(workflow_file)
                        workflow_file_name = file_name.replace('.yml', '').replace('.yaml', '')
                        
                        # Simple name matching - can be improved
                        if workflow_name.lower() in workflow_file_name.lower() or \
                           workflow_file_name.lower() in workflow_name.lower():
                            return features
            
            # If no exact match found, try the first workflow file
            if workflow_files:
                logger.warning(f"No exact match for workflow '{workflow_name}', using first available")
                workflow_path = os.path.join(repo_path, workflow_files[0])
                
                if os.path.exists(workflow_path):
                    with open(workflow_path, 'r', encoding='utf-8') as f:
                        yaml_content = f.read()
                    
                    return self.extractor.parse_yaml_to_vector(yaml_content)
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to extract features for {repo_url}@{commit_sha}: {e}")
            return None
    
    def _extract_features_for_run_cached(self, repo_path: str, workflow_files: List[str], 
                                        workflow_name: str) -> Optional[Dict]:
        """
        Extract features for a specific workflow using already cached repository.
        
        Args:
            repo_path: Path to cached repository
            workflow_files: List of workflow file paths
            workflow_name: Workflow name
            
        Returns:
            Extracted features or None
        """
        try:
            for workflow_file in workflow_files:
                # Read workflow file directly from cached repo
                workflow_path = os.path.join(repo_path, workflow_file)
                
                if os.path.exists(workflow_path):
                    with open(workflow_path, 'r', encoding='utf-8') as f:
                        yaml_content = f.read()
                    
                    # Extract features
                    features = self.extractor.parse_yaml_to_vector(yaml_content)
                    
                    if features:
                        # Match workflow name (strip .yml/.yaml extension)
                        file_name = os.path.basename(workflow_file)
                        workflow_file_name = file_name.replace('.yml', '').replace('.yaml', '')
                        
                        # Simple name matching - can be improved
                        if workflow_name.lower() in workflow_file_name.lower() or \
                           workflow_file_name.lower() in workflow_name.lower():
                            return features
            
            # If no exact match found, try the first workflow file
            if workflow_files:
                workflow_path = os.path.join(repo_path, workflow_files[0])
                
                if os.path.exists(workflow_path):
                    with open(workflow_path, 'r', encoding='utf-8') as f:
                        yaml_content = f.read()
                    
                    return self.extractor.parse_yaml_to_vector(yaml_content)
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to extract features for workflow '{workflow_name}': {e}")
            return None
    
    def run_pipeline(self, repo_urls_file: str, output_file: str = 'data/comprehensive_features.csv',
                    days_back: int = 30, max_runs_per_repo: int = 50) -> pd.DataFrame:
        """
        Run the complete pipeline: mine cost data and extract features.
        
        Args:
            repo_urls_file: File containing repository URLs
            output_file: Output CSV file path
            days_back: Number of days to look back for runs
            max_runs_per_repo: Maximum runs to process per repository
            
        Returns:
            Combined DataFrame with cost data and features
        """
        logger.info("Starting integrated pipeline...")
        
        # Step 1: Mine historical cost data
        logger.info("Step 1: Mining historical cost data...")
        cost_data = self.miner.fetch_run_history(
            repo_urls_file=repo_urls_file,
            output_file='data/temp_raw_run_history.csv',
            days_back=days_back,
            max_runs_per_repo=max_runs_per_repo
        )
        
        if cost_data.empty:
            logger.error("No cost data collected. Pipeline cannot continue.")
            return pd.DataFrame()
        
        logger.info(f"Collected cost data for {len(cost_data)} workflow runs")
        
        # Step 2: Extract features for each run (optimized with batching)
        logger.info("Step 2: Extracting comprehensive features...")
        
        # Prepare repository URL mapping
        with open(repo_urls_file, 'r') as f:
            repo_urls = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        
        repo_url_map = {}
        for url in repo_urls:
            if 'github.com' in url:
                parts = url.strip('/').split('/')
                if len(parts) >= 2:
                    repo_name = f"{parts[-2]}/{parts[-1]}"
                    repo_url_map[repo_name] = url
            else:
                repo_url_map[url] = f"https://github.com/{url}"
        
        # Group runs by repository and commit for efficient processing
        runs_by_repo_commit = {}
        for _, run_row in cost_data.iterrows():
            repo_name = run_row['repo_name']
            commit_sha = run_row['head_sha']
            key = f"{repo_name}@{commit_sha}"
            
            if key not in runs_by_repo_commit:
                runs_by_repo_commit[key] = []
            runs_by_repo_commit[key].append(run_row)
        
        logger.info(f"Grouped {len(cost_data)} runs into {len(runs_by_repo_commit)} repository-commit batches")
        
        # Extract features by batch
        extracted_features = []
        processed_count = 0
        
        for batch_key, runs in runs_by_repo_commit.items():
            repo_name, commit_sha = batch_key.split('@', 1)
            
            # Get repository URL
            repo_url = repo_url_map.get(repo_name)
            if not repo_url:
                logger.warning(f"No URL mapping found for {repo_name}")
                continue
            
            logger.info(f"Processing batch {len(runs_by_repo_commit)}/{len(runs_by_repo_commit)}: {batch_key} ({len(runs)} runs)")
            
            # Cache the repository once for all runs in this batch
            try:
                repo_path = self._get_cached_repo_path(repo_url, commit_sha)
                workflow_files = self._discover_workflow_files_cached(repo_url, commit_sha)
                
                # Process each run in this batch
                batch_success_count = 0
                for run_row in runs:
                    processed_count += 1
                    workflow_name = run_row['workflow_name']
                    
                    # Extract features for this specific workflow
                    features = self._extract_features_for_run_cached(
                        repo_path, workflow_files, workflow_name
                    )
                    
                    if features:
                        # Combine cost data with features
                        combined_data = {
                            # Cost data (target variable)
                            'repo_name': run_row['repo_name'],
                            'run_id': run_row['run_id'],
                            'workflow_name': run_row['workflow_name'],
                            'head_branch': run_row['head_branch'],
                            'head_sha': run_row['head_sha'],
                            'created_at': run_row['created_at'],
                            'updated_at': run_row['updated_at'],
                            'duration_minutes': run_row['duration_minutes'],
                            'total_cost_usd': run_row['total_cost_usd'],
                            'job_count': run_row['job_count'],
                            
                            # Extracted features
                            **features
                        }
                        extracted_features.append(combined_data)
                        batch_success_count += 1
                    else:
                        logger.warning(f"Failed to extract features for {workflow_name}")
                
                logger.info(f"Batch {batch_key}: {batch_success_count}/{len(runs)} runs successful")
                
            except Exception as e:
                logger.error(f"Failed to process batch {batch_key}: {e}")
                # Continue with next batch instead of failing completely
                continue
            
            # Add delay to be respectful between repositories
            if len(runs_by_repo_commit) > 1:
                time.sleep(1)
        
        if not extracted_features:
            logger.error("No features extracted. Pipeline failed.")
            return pd.DataFrame()
        
        # Step 3: Create comprehensive dataset
        logger.info("Step 3: Creating comprehensive dataset...")
        
        comprehensive_df = pd.DataFrame(extracted_features)
        
        # Remove duplicate columns that might exist
        columns_to_keep = list(comprehensive_df.columns)
        columns_to_keep = list(dict.fromkeys(columns_to_keep))  # Remove duplicates while preserving order
        
        comprehensive_df = comprehensive_df[columns_to_keep]
        
        # Save to CSV
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        comprehensive_df.to_csv(output_file, index=False)
        
        # Generate summary statistics
        logger.info(f"\n=== Pipeline Summary ===")
        logger.info(f"Total workflow runs processed: {len(cost_data)}")
        logger.info(f"Successful feature extractions: {len(comprehensive_df)}")
        logger.info(f"Success rate: {len(comprehensive_df)/len(cost_data)*100:.1f}%")
        logger.info(f"Total features extracted: {len([col for col in comprehensive_df.columns if col not in ['repo_name', 'run_id', 'workflow_name', 'head_branch', 'head_sha', 'created_at', 'updated_at']])}")
        logger.info(f"Dataset saved to: {output_file}")
        
        # Cost statistics
        logger.info(f"\n=== Cost Statistics ===")
        logger.info(f"Total cost collected: ${comprehensive_df['total_cost_usd'].sum():.2f}")
        logger.info(f"Average cost per run: ${comprehensive_df['total_cost_usd'].mean():.4f}")
        logger.info(f"Cost range: ${comprehensive_df['total_cost_usd'].min():.4f} - ${comprehensive_df['total_cost_usd'].max():.2f}")
        
        # Feature statistics
        feature_columns = [col for col in comprehensive_df.columns if col not in ['repo_name', 'run_id', 'workflow_name', 'head_branch', 'head_sha', 'created_at', 'updated_at', 'total_cost_usd']]
        logger.info(f"\n=== Feature Statistics ===")
        logger.info(f"Number of feature columns: {len(feature_columns)}")
        logger.info(f"Feature columns: {feature_columns}")
        
        return comprehensive_df
    
    def cleanup(self):
        """Clean up resources including cached repositories."""
        # Clean up cached repositories
        import shutil
        for cache_key, repo_path in self._repo_cache.items():
            try:
                if os.path.exists(repo_path):
                    shutil.rmtree(repo_path)
                    logger.info(f"Cleaned up cached repository: {cache_key}")
            except Exception as e:
                logger.warning(f"Failed to clean up {cache_key}: {e}")
        
        self._repo_cache.clear()
        
        # Clean up extractor
        self.extractor.cleanup()


def main():
    """
    Main function to run the pipeline from command line.
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Integrated pipeline for GitHub workflow cost prediction')
    parser.add_argument('repo_file', help='File containing GitHub repository URLs')
    parser.add_argument('-o', '--output', default='data/comprehensive_features.csv',
                       help='Output CSV file path')
    parser.add_argument('-d', '--days', type=int, default=30,
                       help='Number of days to look back for runs')
    parser.add_argument('-m', '--max-runs', type=int, default=50,
                       help='Maximum runs to process per repository')
    
    args = parser.parse_args()
    
    # Initialize pipeline
    try:
        pipeline = IntegratedPipeline()
    except ValueError as e:
        logger.error(f"Pipeline initialization failed: {e}")
        return 1
    
    try:
        # Run pipeline
        df = pipeline.run_pipeline(
            repo_urls_file=args.repo_file,
            output_file=args.output,
            days_back=args.days,
            max_runs_per_repo=args.max_runs
        )
        
        if df.empty:
            logger.error("Pipeline failed to generate dataset")
            return 1
        
        logger.info("🎉 Pipeline completed successfully!")
        logger.info(f"Dataset ready for exploratory data analysis: {args.output}")
        
        return 0
        
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}")
        return 1
    finally:
        pipeline.cleanup()


if __name__ == "__main__":
    exit(main())
