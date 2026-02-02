#!/usr/bin/env python3
"""
The Historic Miner (Ground Truth) Component
Collects actual cost data from GitHub workflow runs to build the target variable.
"""

import os
import time
import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from github import Github, GithubException, UnknownObjectException
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Cost calculation constants
OS_RATES = {
    'linux': 0.008,    # $0.008 per minute
    'windows': 0.016,  # $0.016 per minute  
    'macos': 0.08      # $0.08 per minute
}

class HistoricMiner:
    """
    The Historic Miner collects actual cost data from GitHub workflow runs.
    This serves as the ground truth data for training the cost prediction model.
    """
    
    def __init__(self, github_token: Optional[str] = None):
        """
        Initialize the miner with GitHub credentials.
        
        Args:
            github_token: GitHub personal access token. If None, tries to get from environment.
        """
        self.github_token = github_token or os.getenv('GITHUB_TOKEN')
        if not self.github_token:
            raise ValueError("GitHub token is required. Set GITHUB_TOKEN environment variable or pass token parameter.")
        
        self.github = Github(self.github_token)
        self.rate_limit_remaining = 5000
        self.rate_limit_reset = 0
        
    def _check_rate_limit(self):
        """Check and handle GitHub API rate limiting with exponential backoff."""
        try:
            rate_limit = self.github.get_rate_limit()
            # Handle different PyGithub API versions
            if hasattr(rate_limit, 'core'):
                # Newer versions
                self.rate_limit_remaining = rate_limit.core.remaining
                self.rate_limit_reset = rate_limit.core.reset.timestamp()
            elif hasattr(rate_limit, 'remaining'):
                # Some versions have it directly
                self.rate_limit_remaining = rate_limit.remaining
                self.rate_limit_reset = rate_limit.reset.timestamp()
            elif hasattr(rate_limit, 'search'):
                # Fallback to search rate limit
                self.rate_limit_remaining = rate_limit.search.remaining
                self.rate_limit_reset = rate_limit.search.reset.timestamp()
            else:
                # Unknown structure, assume we're fine
                self.rate_limit_remaining = 1000
                self.rate_limit_reset = time.time() + 3600
            
            if self.rate_limit_remaining < 100:
                wait_time = max(60, self.rate_limit_reset - time.time() + 60)
                logger.warning(f"Rate limit low ({self.rate_limit_remaining}). Waiting {wait_time:.0f} seconds...")
                time.sleep(wait_time)
                
        except Exception as e:
            logger.warning(f"Could not check rate limit: {e}")
            # Set conservative defaults
            self.rate_limit_remaining = 1000
            self.rate_limit_reset = time.time() + 3600
    
    def _extract_os_from_runner(self, runner_name: str) -> str:
        """
        Extract OS type from runner name.
        
        Args:
            runner_name: GitHub Actions runner name (e.g., 'ubuntu-latest', 'windows-2019')
            
        Returns:
            OS type: 'linux', 'windows', 'macos', or 'linux' as default
        """
        runner_lower = runner_name.lower()
        if 'windows' in runner_lower:
            return 'windows'
        elif 'macos' in runner_lower or 'mac' in runner_lower:
            return 'macos'
        else:
            return 'linux'  # Default to linux for ubuntu and other runners
    
    def _calculate_workflow_cost(self, duration_minutes: float, os_type: str) -> float:
        """
        Calculate the actual cost of a workflow run.
        
        Args:
            duration_minutes: Duration in minutes
            os_type: Operating system type
            
        Returns:
            Cost in USD
        """
        base_rate = OS_RATES.get(os_type, OS_RATES['linux'])
        return duration_minutes * base_rate
    
    def _fetch_workflow_runs_for_repo(self, repo_full_name: str, days_back: int = 30, max_runs: int = 100) -> List[Dict]:
        """
        Fetch workflow run data for a specific repository.
        
        Args:
            repo_full_name: Repository name in format 'owner/repo'
            days_back: Number of days to look back for runs
            max_runs: Maximum number of runs to fetch per repo
            
        Returns:
            List of dictionaries containing run data
        """
        logger.info(f"Fetching workflow runs for {repo_full_name}")
        
        try:
            self._check_rate_limit()
            repo = self.github.get_repo(repo_full_name)
            
            # Calculate date threshold
            since_date = datetime.now() - timedelta(days=days_back)
            
            # Get workflow runs (filter for completed runs, then filter for success)
            try:
                runs = repo.get_workflow_runs(
                    status='completed',
                    created=f">={since_date.isoformat()}"
                )
            except TypeError:
                # Fallback for older PyGithub versions
                runs = repo.get_workflow_runs()
            
            
            run_data = []
            processed_count = 0
            
            for run in runs:
                if processed_count >= max_runs:
                    break
                    
                try:
                    # Only include successful runs
                    if run.conclusion != 'success':
                        continue
                        
                    # Extract job details for accurate timing and OS info
                    jobs = run.jobs()
                    total_cost = 0.0
                    job_details = []
                    
                    for job in jobs:
                        if job.conclusion == 'success':
                            # Handle different PyGithub versions for job duration
                            if hasattr(job, 'duration'):
                                duration_seconds = job.duration
                            elif hasattr(job, 'started_at') and hasattr(job, 'completed_at'):
                                if job.started_at and job.completed_at:
                                    duration_seconds = (job.completed_at - job.started_at).total_seconds()
                                else:
                                    duration_seconds = 0
                            else:
                                # Fallback: use workflow run duration divided by job count
                                duration_seconds = 0
                            
                            # Convert duration from seconds to minutes
                            duration_minutes = duration_seconds / 60.0 if duration_seconds > 0 else 0
                            os_type = self._extract_os_from_runner(job.runner_name or 'ubuntu-latest')
                            job_cost = self._calculate_workflow_cost(duration_minutes, os_type)
                            
                            total_cost += job_cost
                            job_details.append({
                                'job_name': job.name,
                                'duration_minutes': duration_minutes,
                                'os_type': os_type,
                                'job_cost': job_cost
                            })
                    
                    if total_cost > 0:  # Only include runs with actual cost
                        run_data.append({
                            'repo_name': repo_full_name,
                            'run_id': run.id,
                            'workflow_name': run.name,
                            'head_branch': run.head_branch,
                            'head_sha': run.head_sha,
                            'created_at': run.created_at.isoformat(),
                            'updated_at': run.updated_at.isoformat(),
                            'duration_minutes': sum(job['duration_minutes'] for job in job_details),
                            'total_cost_usd': total_cost,
                            'job_count': len(job_details),
                            'job_details': job_details
                        })
                    
                    processed_count += 1
                    
                    if processed_count % 10 == 0:
                        logger.info(f"Processed {processed_count} runs for {repo_full_name}")
                        
                except Exception as e:
                    logger.warning(f"Error processing run {run.id} in {repo_full_name}: {e}")
                    continue
            
            logger.info(f"Successfully fetched {len(run_data)} runs from {repo_full_name}")
            return run_data
            
        except UnknownObjectException:
            logger.error(f"Repository {repo_full_name} not found or not accessible")
            return []
        except GithubException as e:
            logger.error(f"GitHub API error for {repo_full_name}: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error fetching runs for {repo_full_name}: {e}")
            return []
    
    def fetch_run_history(self, repo_urls_file: str, output_file: str = 'data/raw_run_history.csv', 
                         days_back: int = 30, max_runs_per_repo: int = 100) -> pd.DataFrame:
        """
        Main function to fetch run history from multiple repositories.
        
        Args:
            repo_urls_file: Path to file containing repository URLs (one per line)
            output_file: Output CSV file path
            days_back: Number of days to look back for runs
            max_runs_per_repo: Maximum runs to fetch per repository
            
        Returns:
            DataFrame containing all run history
        """
        logger.info(f"Starting historic mining from {repo_urls_file}")
        
        # Read repository URLs
        try:
            with open(repo_urls_file, 'r') as f:
                repo_urls = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        except FileNotFoundError:
            logger.error(f"Repository URLs file not found: {repo_urls_file}")
            return pd.DataFrame()
        
        # Convert URLs to full names (owner/repo)
        repo_names = []
        for url in repo_urls:
            if 'github.com' in url:
                # Extract owner/repo from URL
                parts = url.strip('/').split('/')
                if len(parts) >= 2:
                    repo_name = f"{parts[-2]}/{parts[-1]}"
                    repo_names.append(repo_name)
            else:
                # Assume it's already in owner/repo format
                repo_names.append(url)
        
        logger.info(f"Processing {len(repo_names)} repositories")
        
        all_run_data = []
        
        for i, repo_name in enumerate(repo_names, 1):
            logger.info(f"Processing repository {i}/{len(repo_names)}: {repo_name}")
            
            run_data = self._fetch_workflow_runs_for_repo(
                repo_name, 
                days_back=days_back, 
                max_runs=max_runs_per_repo
            )
            all_run_data.extend(run_data)
            
            # Add delay between repositories to be respectful to the API
            if i < len(repo_names):
                time.sleep(2)
        
        if not all_run_data:
            logger.warning("No run data collected. Check repository access and API permissions.")
            return pd.DataFrame()
        
        # Convert to DataFrame and save
        df = pd.DataFrame(all_run_data)
        
        # Reorder columns for better readability
        columns_order = [
            'repo_name', 'run_id', 'workflow_name', 'head_branch', 'head_sha',
            'created_at', 'updated_at', 'duration_minutes', 'total_cost_usd', 
            'job_count'
        ]
        df = df[columns_order]
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        # Save to CSV
        df.to_csv(output_file, index=False)
        logger.info(f"Saved {len(df)} run records to {output_file}")
        
        # Print summary statistics
        logger.info(f"\n=== Mining Summary ===")
        logger.info(f"Total repositories processed: {len(repo_names)}")
        logger.info(f"Total workflow runs collected: {len(df)}")
        logger.info(f"Total cost collected: ${df['total_cost_usd'].sum():.2f}")
        logger.info(f"Average cost per run: ${df['total_cost_usd'].mean():.4f}")
        logger.info(f"Cost range: ${df['total_cost_usd'].min():.4f} - ${df['total_cost_usd'].max():.2f}")
        
        return df


def main():
    """
    Main function to run the historic miner from command line.
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Mine GitHub workflow run history for cost prediction')
    parser.add_argument('repo_file', help='File containing GitHub repository URLs')
    parser.add_argument('-o', '--output', default='data/raw_run_history.csv', 
                       help='Output CSV file path')
    parser.add_argument('-d', '--days', type=int, default=30,
                       help='Number of days to look back for runs')
    parser.add_argument('-m', '--max-runs', type=int, default=100,
                       help='Maximum runs to fetch per repository')
    
    args = parser.parse_args()
    
    # Initialize miner
    try:
        miner = HistoricMiner()
    except ValueError as e:
        logger.error(f"Initialization failed: {e}")
        return 1
    
    # Fetch run history
    df = miner.fetch_run_history(
        repo_urls_file=args.repo_file,
        output_file=args.output,
        days_back=args.days,
        max_runs_per_repo=args.max_runs
    )
    
    if df.empty:
        logger.error("No data was collected. Check the logs for errors.")
        return 1
    
    logger.info("Historic mining completed successfully!")
    return 0


if __name__ == "__main__":
    exit(main())
