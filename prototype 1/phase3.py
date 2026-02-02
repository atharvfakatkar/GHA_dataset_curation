import os
import yaml
import logging
import pandas as pd
import time
from typing import Dict, List, Any
from github import Github, GithubException
from dotenv import load_dotenv

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

class TimeTravelExtractor:
    def __init__(self, token: str):
        if not token:
            raise ValueError("GitHub Token not found.")
        self.g = Github(token)
        # Cache to prevent fetching the same SHA twice
        self.processed_shas = set()

    def _normalize_os(self, runs_on: Any) -> str:
        """Maps 'runs-on' tags to standard categories."""
        runs_on_str = str(runs_on).lower()
        if 'windows' in runs_on_str or 'win' in runs_on_str:
            return 'windows'
        elif 'macos' in runs_on_str or 'mac' in runs_on_str:
            return 'macos'
        return 'linux'

    def _calculate_matrix_size(self, job_config: Dict) -> int:
        """Calculates job expansion from matrix strategy."""
        strategy = job_config.get('strategy', {})
        matrix = strategy.get('matrix', {})
        if not matrix: return 1
        
        total = 1
        for key, value in matrix.items():
            if key not in ['include', 'exclude'] and isinstance(value, list):
                total *= len(value)
        return total

    def parse_yaml(self, content: str, filename: str) -> List[Dict]:
        """Extracts features from raw YAML string."""
        features = []
        try:
            workflow = yaml.safe_load(content)
            if not workflow or 'jobs' not in workflow: return []
            
            for job_id, job_config in workflow['jobs'].items():
                if 'runs-on' not in job_config: continue

                features.append({
                    'filename': filename,
                    'job_name': job_id,
                    'runner_os': self._normalize_os(job_config['runs-on']),
                    'matrix_size': self._calculate_matrix_size(job_config),
                    'step_count': len(job_config.get('steps', [])),
                    'uses_action_count': sum(1 for s in job_config.get('steps', []) if 'uses' in s),
                    'has_services': 1 if 'services' in job_config else 0
                })
        except yaml.YAMLError:
            pass
        return features

    def process_historical_commits(self, history_file: str, output_file: str):
        """
        Reads Phase 2 history and fetches the exact YAML version for each run.
        """
        if not os.path.exists(history_file):
            logger.error(f"History file {history_file} not found. Run Phase 2 first.")
            return

        # 1. Load History and find unique commits
        df_history = pd.read_csv(history_file)
        
        # We need unique pairs of (repo_name, head_sha)
        # This drastically reduces API calls (e.g., 10 runs might share 1 commit)
        unique_targets = df_history[['repo_name', 'head_sha']].drop_duplicates()
        
        logger.info(f"Found {len(unique_targets)} unique commits to analyze from history.")

        all_features = []
        
        # 2. Iterate through history
        for _, row in unique_targets.iterrows():
            repo_name = row['repo_name']
            commit_sha = row['head_sha']
            
            # Skip if we already processed this SHA (in case of restart or duplicates)
            cache_key = f"{repo_name}_{commit_sha}"
            if cache_key in self.processed_shas:
                continue

            try:
                repo = self.g.get_repo(repo_name)
                logger.info(f"Time-Traveling to {repo_name} @ {commit_sha[:7]}...")
                
                # GET CONTENTS AT SPECIFIC SHA (The Core Logic)
                try:
                    contents = repo.get_contents(".github/workflows", ref=commit_sha)
                except Exception:
                    logger.warning(f"No workflows found at {commit_sha[:7]}")
                    continue

                if not isinstance(contents, list): contents = [contents]

                for content_file in contents:
                    if content_file.name.endswith(('.yml', '.yaml')):
                        yaml_raw = content_file.decoded_content.decode("utf-8")
                        file_feats = self.parse_yaml(yaml_raw, content_file.name)
                        
                        # Tag features with the SHA so they can join later
                        for f in file_feats:
                            f['repo_name'] = repo_name
                            f['commit_sha'] = commit_sha
                            all_features.append(f)

                self.processed_shas.add(cache_key)
                
                # Respect API Rate Limits
                time.sleep(0.5) 

            except GithubException as e:
                logger.error(f"Error accessing {repo_name} @ {commit_sha}: {e}")

        # 3. Save Results
        if all_features:
            df = pd.DataFrame(all_features)
            df.to_csv(output_file, index=False)
            logger.info(f"Success! Saved {len(df)} feature records to {output_file}")
            print("\nPreview of Time-Traveled Features:")
            print(df[['commit_sha', 'runner_os', 'matrix_size']].head())
        else:
            logger.warning("No features extracted.")

if __name__ == "__main__":
    # Input: The output from Phase 2
    PHASE_2_FILE = "master_dataset.csv"
    # Output: The input for Phase 4
    PHASE_3_OUTPUT = "features_dataset.csv"
    
    extractor = TimeTravelExtractor(GITHUB_TOKEN)
    extractor.process_historical_commits(PHASE_2_FILE, PHASE_3_OUTPUT)