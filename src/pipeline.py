#!/usr/bin/env python3
"""
Integrated Async Pipeline: Historic Miner + Vacuum Extractor + Code Complexity
Orchestrates the asynchronous collection of runs, workflows, and code complexity.
"""

import os
import shutil
import logging
import asyncio
import aiohttp
import pandas as pd
import tempfile
from typing import Dict, Optional, Any

from src.miner import HistoricMiner
from src.extractor import VacuumExtractor
from src.code_complexity import CodeComplexityExtractor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class IntegratedPipeline:
    def __init__(self, github_token: Optional[str] = None):
        self.github_token = github_token or os.getenv('GITHUB_TOKEN')
        self.miner = HistoricMiner(self.github_token)
        self.extractor = VacuumExtractor(self.github_token)
        self.base_temp_dir = tempfile.mkdtemp(prefix="pipeline_downloads_")
        logger.info("Initialized Async Integrated Pipeline")

    def _infer_primary_language(self, source_dir: str) -> Optional[str]:
        """
        Infer a primary technology/language for the repository at a given commit
        based on the extensions of the downloaded source files.
        """
        # Map file extensions to high-level language labels
        ext_to_lang = {
            ".py": "python",
            ".java": "java",
            ".js": "javascript",
            ".ts": "typescript",
            ".c": "c",
            ".cpp": "cpp",
            ".cs": "csharp",
            ".go": "go",
            ".rb": "ruby",
            ".php": "php",
            ".swift": "swift",
            ".kt": "kotlin",
            ".rs": "rust",
        }

        counts: Dict[str, int] = {}
        for root, _, files in os.walk(source_dir):
            for name in files:
                _, ext = os.path.splitext(name)
                lang = ext_to_lang.get(ext.lower())
                if not lang:
                    continue
                counts[lang] = counts.get(lang, 0) + 1

        if not counts:
            return None

        # Pick the language with the highest file count
        primary_lang = max(counts.items(), key=lambda kv: kv[1])[0]
        return primary_lang

    async def _extract_features_for_commits(self, unique_commits: list) -> dict:
        """Asynchronously process a list of unique (repo_name, commit_sha) pairs."""
        results = {}
        total = len(unique_commits)
        processed = 0
        
        async def process_commit(session, repo_name, commit_sha):
            workflows, source_dir = await self.extractor.prepare_repository(
                session, repo_name, commit_sha, self.base_temp_dir
            )
            
            if not workflows:
                return repo_name, commit_sha, None
                
            complexity = 0.0
            primary_language: Optional[str] = None
            if source_dir:
                # Compute complexity and primary language from the code snapshot
                # corresponding exactly to this (repo_name, commit_sha).
                comp_extractor = CodeComplexityExtractor(source_dir)
                complexity = comp_extractor.extract_complexity()
                primary_language = self._infer_primary_language(source_dir)
                # Clean up source dir immediately to save disk space
                shutil.rmtree(source_dir, ignore_errors=True)
                
            # Parse all workflows and store features
            workflow_features = {}
            for path, content in workflows.items():
                feats = self.extractor.parse_yaml_to_vector(content)
                if feats:
                    workflow_features[path] = feats

            return repo_name, commit_sha, {
                'complexity': complexity,
                'primary_language': primary_language,
                'workflows': workflow_features
            }

        # Per-request headers (including rotating tokens) are handled inside
        # the extractor and miner; no fixed Authorization header is needed here.
        timeout = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            tasks = [process_commit(session, r, c) for r, c in unique_commits]
            
            # Process in chunks to avoid overloading memory and APIs
            chunk_size = 20
            for i in range(0, len(tasks), chunk_size):
                chunk = tasks[i:i + chunk_size]
                chunk_results = await asyncio.gather(*chunk, return_exceptions=True)
                
                for result in chunk_results:
                    if isinstance(result, Exception):
                        logger.warning(f"Error while processing commit: {result}")
                        continue
                    repo_name, commit_sha, data = result
                    if data:
                        results[(repo_name, commit_sha)] = data
                    processed += 1

                logger.info(f"Feature extraction progress: {processed}/{total} commits processed")
                        
        return results

    def run_pipeline(
        self,
        repo_urls_file: str,
        output_file: str = 'data/comprehensive_features.csv',
        days_back: int = 5,
        max_runs_per_repo: int = 20
    ) -> pd.DataFrame:
        
        logger.info("Starting pipeline execution...")

        # Step 1: Mine cost data
        cost_data = self.miner.fetch_run_history(
            repo_urls_file=repo_urls_file,
            output_file='data/temp_raw_run_history.csv',
            days_back=days_back,
            max_runs_per_repo=max_runs_per_repo
        )

        if cost_data.empty:
            logger.error("No cost data collected.")
            return pd.DataFrame()

        # Step 2: Extract unique commits to avoid redundant API calls & downloads
        unique_commits = cost_data[['repo_name', 'head_sha']].drop_duplicates().values.tolist()
        logger.info(f"Extracting features for {len(unique_commits)} unique commits...")

        # Step 3: Run async extraction safely
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
        extracted_data = loop.run_until_complete(self._extract_features_for_commits(unique_commits))

        # Step 4: Combine features with run data
        extracted_rows = []
        for _, run_row in cost_data.iterrows():
            repo_name = run_row['repo_name']
            commit_sha = run_row['head_sha']
            
            commit_data = extracted_data.get((repo_name, commit_sha))
            if not commit_data or not commit_data['workflows']:
                continue
                
            code_complexity = commit_data['complexity']
            primary_language = commit_data.get('primary_language')
            
            for workflow_path, features in commit_data['workflows'].items():
                combined = {
                    # Target and core run metadata
                    'total_cost_usd': run_row['total_cost_usd'],
                    'duration_minutes': run_row.get('duration_minutes'),
                    'repo_name': repo_name,
                    'head_sha': commit_sha,
                    'workflow_name': run_row['workflow_name'],
                    **features,
                    'code_complexity': code_complexity,
                    'primary_language': primary_language
                }
                extracted_rows.append(combined)

        if not extracted_rows:
            logger.error("No features extracted.")
            return pd.DataFrame()

        df = pd.DataFrame(extracted_rows)
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        df.to_csv(output_file, index=False)

        logger.info(f"Dataset saved to {output_file}")
        logger.info(f"Total samples: {len(df)}")
        
        return df

    def cleanup(self):
        if os.path.exists(self.base_temp_dir):
            shutil.rmtree(self.base_temp_dir, ignore_errors=True)
        logger.info("Pipeline cleanup complete.")