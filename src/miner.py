#!/usr/bin/env python3
"""
The Async Historic Miner Component
Collects actual cost data from GitHub workflow runs concurrently using aiohttp.
Replaces the synchronous PyGithub implementation for massive speedups.
"""

import os
import time
import asyncio
import aiohttp
import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from itertools import cycle
from dotenv import load_dotenv
import aiohttp

# Load environment variables
load_dotenv()

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
    Upgraded to use async I/O to fetch jobs concurrently without cloning.
    """
    
    def __init__(self, github_token: Optional[str] = None):
        """
        Initialize miner with support for token rotation.
        If a single github_token is provided, it is used for all requests.
        Otherwise, the miner looks for GITHUB_TOKEN1, GITHUB_TOKEN2, GITHUB_TOKEN3
        in the environment and rotates through whichever are set.
        """
        # Collect available tokens
        tokens: List[str] = []
        if github_token:
            tokens.append(github_token)
        else:
            for i in range(1, 4):
                tok = os.getenv(f"GITHUB_TOKEN{i}")
                if tok:
                    tokens.append(tok)

        if not tokens:
            # Fallback to legacy single-token env var
            legacy = os.getenv("GITHUB_TOKEN")
            if legacy:
                tokens.append(legacy)

        if not tokens:
            raise ValueError(
                "GitHub token is required. Set GITHUB_TOKEN or GITHUB_TOKEN1-3 environment variables."
            )

        # Round-robin iterator over tokens
        self._tokens = tokens
        self._token_cycle = cycle(self._tokens)

        # Base headers without Authorization; each request will get its own token
        self.base_headers = {
            "Accept": "application/vnd.github.v3+json",
        }

        # Semaphore to prevent hitting GitHub's secondary rate limits (abuse limits)
        self.semaphore = asyncio.Semaphore(15) 

    def _next_headers(self) -> Dict[str, str]:
        """Return headers with the next token in round robin."""
        token = next(self._token_cycle)
        headers = dict(self.base_headers)
        headers["Authorization"] = f"token {token}"
        return headers
        
    def _extract_os_from_runner(self, labels: List[str], runner_name: str) -> str:
        text_to_check = (str(runner_name) + " " + " ".join(labels)).lower()
        if 'windows' in text_to_check:
            return 'windows'
        elif 'macos' in text_to_check or 'mac' in text_to_check:
            return 'macos'
        else:
            return 'linux'
            
    async def _fetch_json(self, session: aiohttp.ClientSession, url: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """Fetch JSON from GitHub API with exponential backoff for rate limits."""
        async with self.semaphore:
            for attempt in range(3):
                # Rotate token per attempt to spread load across tokens
                headers = self._next_headers()
                async with session.get(url, params=params, headers=headers) as response:
                    if response.status == 200:
                        return await response.json()
                    elif response.status in (403, 429):
                        reset_timestamp = int(response.headers.get('X-RateLimit-Reset', time.time() + 60))
                        sleep_time = max(1, reset_timestamp - time.time() + 1)
                        logger.warning(f"Rate limited on {url}. Sleeping for {sleep_time:.0f}s...")
                        await asyncio.sleep(sleep_time)
                    elif response.status == 404:
                        return None
                    else:
                        logger.error(f"Failed to fetch {url}: HTTP {response.status}")
                        await asyncio.sleep(2 ** attempt)
            return None

    async def _process_run(self, session: aiohttp.ClientSession, owner: str, repo: str, run: Dict) -> Optional[Dict]:
        """Fetch jobs for a specific run and calculate cost."""
        if run['conclusion'] != 'success':
            return None
            
        jobs_url = run['jobs_url']
        jobs_data = await self._fetch_json(session, jobs_url)
        
        if not jobs_data or not jobs_data.get('jobs'):
            return None
            
        total_cost = 0.0
        job_details = []
        
        for job in jobs_data['jobs']:
            if job['conclusion'] == 'success' and job['started_at'] and job['completed_at']:
                started = datetime.fromisoformat(job['started_at'].replace('Z', '+00:00'))
                completed = datetime.fromisoformat(job['completed_at'].replace('Z', '+00:00'))
                duration_minutes = max(0, (completed - started).total_seconds() / 60.0)
                
                os_type = self._extract_os_from_runner(job.get('labels', []), job.get('runner_name', ''))
                job_cost = duration_minutes * OS_RATES.get(os_type, OS_RATES['linux'])
                
                total_cost += job_cost
                job_details.append({
                    'job_name': job['name'],
                    'duration_minutes': duration_minutes,
                    'os_type': os_type,
                    'job_cost': job_cost
                })
        
        if total_cost > 0:
            return {
                'repo_name': f"{owner}/{repo}",
                'run_id': run['id'],
                'workflow_name': run['name'],
                'head_branch': run['head_branch'],
                'head_sha': run['head_sha'],
                'created_at': run['created_at'],
                'updated_at': run['updated_at'],
                'duration_minutes': sum(j['duration_minutes'] for j in job_details),
                'total_cost_usd': total_cost,
                'job_count': len(job_details)
            }
        return None

    async def _fetch_repo_runs_async(self, session: aiohttp.ClientSession, repo_full_name: str, days_back: int, max_runs: int) -> List[Dict]:
        """Fetch workflow runs for a repository."""
        logger.info(f"Fetching async workflow runs for {repo_full_name}")
        parts = repo_full_name.strip('/').split('/')
        if len(parts) < 2:
            return []
            
        owner, repo = parts[-2], parts[-1]
        since_date = (datetime.utcnow() - timedelta(days=days_back)).isoformat() + "Z"
        
        url = f"https://api.github.com/repos/{owner}/{repo}/actions/runs"
        params = {
            'status': 'success',
            'created': f">={since_date}",
            'per_page': min(100, max_runs)
        }
        
        runs_data = await self._fetch_json(session, url, params)
        if not runs_data or not runs_data.get('workflow_runs'):
            return []
            
        # Concurrently fetch jobs for all runs in this repo
        tasks = [
            self._process_run(session, owner, repo, run) 
            for run in runs_data['workflow_runs'][:max_runs]
        ]
        
        results = await asyncio.gather(*tasks)
        valid_results = [r for r in results if r]
        logger.info(f"Successfully processed {len(valid_results)} runs for {repo_full_name}")
        return valid_results

    async def _run_pipeline_async(self, repo_names: List[str], days_back: int, max_runs_per_repo: int) -> List[Dict]:
        """Main async orchestration loop for all repositories.

        To respect rate limits when using three API tokens, we intentionally
        process repositories in batches of three at a time.
        """
        # Do not set a fixed Authorization header here; each request
        # will pull from the rotating token headers in _fetch_json.
        timeout = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            all_results: List[List[Dict]] = []

            batch_size = 3
            for i in range(0, len(repo_names), batch_size):
                batch = repo_names[i:i + batch_size]
                logger.info(f"Processing repository batch {i // batch_size + 1} "
                            f"({len(batch)} repos): {batch}")

                tasks = [
                    self._fetch_repo_runs_async(session, repo, days_back, max_runs_per_repo)
                    for repo in batch
                ]

                batch_results = await asyncio.gather(*tasks)
                all_results.extend(batch_results)

            # Flatten the list of lists
            return [item for sublist in all_results for item in sublist]

    def fetch_run_history(self, repo_urls_file: str, output_file: str = 'data/raw_run_history.csv', 
                         days_back: int = 30, max_runs_per_repo: int = 100) -> pd.DataFrame:
        """
        Synchronous wrapper to start the async loop.
        """
        logger.info(f"Starting async historic mining from {repo_urls_file}")
        
        try:
            with open(repo_urls_file, 'r') as f:
                repo_names = []
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        # Normalize URL to owner/repo format
                        if 'github.com' in line:
                            parts = line.split('github.com/')[-1].split('/')
                            if len(parts) >= 2:
                                repo_names.append(f"{parts[0]}/{parts[1]}")
                        else:
                            repo_names.append(line)
        except FileNotFoundError:
            logger.error(f"Repository URLs file not found: {repo_urls_file}")
            return pd.DataFrame()
        
        # Run async event loop
        loop = asyncio.get_event_loop()
        all_run_data = loop.run_until_complete(
            self._run_pipeline_async(repo_names, days_back, max_runs_per_repo)
        )
        
        if not all_run_data:
            logger.warning("No run data collected.")
            return pd.DataFrame()
        
        df = pd.DataFrame(all_run_data)
        columns_order = [
            'repo_name', 'run_id', 'workflow_name', 'head_branch', 'head_sha',
            'created_at', 'updated_at', 'duration_minutes', 'total_cost_usd', 
            'job_count'
        ]
        
        # Filter existing columns to avoid errors if some are missing
        columns_order = [c for c in columns_order if c in df.columns]
        df = df[columns_order]
        
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        df.to_csv(output_file, index=False)
        
        logger.info(f"Saved {len(df)} run records to {output_file}")
        logger.info(f"Total cost collected: ${df['total_cost_usd'].sum():.2f}")
        return df