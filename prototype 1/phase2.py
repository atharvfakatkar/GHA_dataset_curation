import os
import time
import logging
from typing import List, Dict, Any
from datetime import datetime
import pandas as pd
from github import Github, GithubException
from dotenv import load_dotenv

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load Environment Variables
load_dotenv()
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

class GitHubCostCollector:
    def __init__(self, token: str):
        if not token:
            raise ValueError("GitHub Token not found. Please set GITHUB_TOKEN in .env file.")
        self.g = Github(token)
        # Pricing constants based on Paper
        self.BASE_PRICE_PER_MIN = 0.008
        self.OS_MULTIPLIERS = {
            'linux': 1,
            'windows': 2,
            'macos': 10
        }

    def get_os_multiplier(self, labels: List[str], job_name: str) -> int:
        """
        Determines the OS multiplier based on runner labels (Ground Truth).
        Falls back to job name heuristic if labels are missing.
        """
        # 1. Check Labels (Most Accurate)
        if labels:
            # labels is a list of strings, e.g., ['macos-latest']
            labels_str = " ".join(labels).lower()
            
            if 'macos' in labels_str or 'mac' in labels_str or 'darwin' in labels_str:
                return self.OS_MULTIPLIERS['macos']
            if 'windows' in labels_str or 'win' in labels_str:
                return self.OS_MULTIPLIERS['windows']
            if 'ubuntu' in labels_str or 'linux' in labels_str:
                return self.OS_MULTIPLIERS['linux']

        # 2. Fallback to Job Name Heuristic
        job_name_lower = job_name.lower()
        if 'macos' in job_name_lower or 'mac' in job_name_lower:
            return self.OS_MULTIPLIERS['macos']
        if 'windows' in job_name_lower or 'win' in job_name_lower:
            return self.OS_MULTIPLIERS['windows']
            
        # Default to Linux if no clues found
        return self.OS_MULTIPLIERS['linux']

    def calculate_job_cost(self, duration_seconds: float, multiplier: int) -> float:
        """
        Implements Cost Formula: [t * f] * 0.008
        """
        duration_minutes = duration_seconds / 60.0
        return (duration_minutes * multiplier) * self.BASE_PRICE_PER_MIN

    def process_repository(self, repo_url: str, limit_runs: int = 100) -> List[Dict]:
        """
        Scrapes workflow runs and job details from a single repository.
        """
        data_records = []
        try:
            # Safer URL parsing
            clean_url = repo_url.rstrip("/")
            if "github.com/" in clean_url:
                repo_name = clean_url.split("github.com/")[-1]
            else:
                repo_name = clean_url # Handle case where user just gives "owner/repo"

            repo = self.g.get_repo(repo_name)
            logger.info(f"Processing Repository: {repo_name}")

            runs = repo.get_workflow_runs(status="completed")
            
            count = 0
            for run in runs:
                if count >= limit_runs:
                    break
                
                run_meta = {
                    'repo_name': repo_name,
                    'run_id': run.id,
                    'workflow_name': run.name,
                    'event': run.event,
                    'status': run.status,
                    'conclusion': run.conclusion,
                    'head_sha': run.head_sha,
                    'run_attempt': run.run_attempt,
                    'created_at': run.created_at
                }

                try:
                    jobs = run.jobs()
                    
                    for job in jobs:
                        if job.started_at and job.completed_at:
                            duration = (job.completed_at - job.started_at).total_seconds()
                            duration = max(0, duration) # Prevent negative duration edge cases
                            
                            # FIX: Pass the actual job labels to the helper function
                            multiplier = self.get_os_multiplier(job.labels, job.name)
                            
                            cost = self.calculate_job_cost(duration, multiplier)

                            job_record = run_meta.copy()
                            job_record.update({
                                'level': 'job',
                                'job_id': job.id,
                                'job_name': job.name,
                                'labels': job.labels, # Store labels for verification
                                'os_multiplier': multiplier,
                                'duration_seconds': duration,
                                'financial_cost_usd': cost,
                                'step_count': len(job.steps) if job.steps else 0
                            })
                            data_records.append(job_record)
                    
                    count += 1
                    
                except GithubException as e:
                    logger.error(f"Error fetching jobs for run {run.id}: {e}")
                    continue

        except GithubException as e:
            logger.error(f"Failed to access repository {repo_url}: {e}")
        
        return data_records

    def run_pipeline(self, repo_urls: List[str], output_file: str = "master_dataset.csv"):
        all_data = []
        for url in repo_urls:
            repo_data = self.process_repository(url)
            all_data.extend(repo_data)
        
        df = pd.DataFrame(all_data)
        
        if not df.empty:
            df.to_csv(output_file, index=False)
            logger.info(f"Successfully saved {len(df)} records to {output_file}")
        else:
            logger.warning("No data collected.")

if __name__ == "__main__":
    # Add your repository URL here
    target_repos = [
        # "https://github.com/atharvfakatkar/trialPretextView"
        "https://github.com/actions/starter-workflows", 
        # "https://github.com/your-username/your-repo"
    ]
    
    # Ensure token is present before running
    if GITHUB_TOKEN:
        collector = GitHubCostCollector(GITHUB_TOKEN)
        collector.run_pipeline(target_repos)
    else:
        logger.error("Please set GITHUB_TOKEN in your .env file")