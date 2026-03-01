#!/usr/bin/env python3
"""
The Async Vacuum Extractor (Broad-Spectrum Features) Component
Extracts comprehensive features from GitHub workflow YAML files for zero-run cost prediction.
Upgraded to use async I/O, Git Trees API, and raw file downloads to avoid git cloning.
"""

import os
import yaml
import logging
import asyncio
import aiohttp
from typing import Dict, List, Optional, Any, Tuple
from itertools import cycle
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class VacuumExtractor:
    """
    The Vacuum Extractor performs comprehensive feature extraction from GitHub workflow YAML files.
    Upgraded to fetch files concurrently in-memory and download only required source files.
    """
    
    # Valid source code extensions for Lizard code complexity analysis
    SOURCE_EXTENSIONS = {
        '.py', '.java', '.js', '.ts', '.c', '.cpp', '.cs', '.go', '.rb', '.php', '.swift', '.kt', '.rs'
    }

    def __init__(self, github_token: Optional[str] = None):
        """
        Initialize extractor with support for token rotation.
        Mirrors the behavior of HistoricMiner: if a single token is provided,
        it is used for all requests; otherwise looks for GITHUB_TOKEN1-3.
        """
        tokens: List[str] = []
        if github_token:
            tokens.append(github_token)
        else:
            for i in range(1, 4):
                tok = os.getenv(f"GITHUB_TOKEN{i}")
                if tok:
                    tokens.append(tok)

        if not tokens:
            legacy = os.getenv("GITHUB_TOKEN")
            if legacy:
                tokens.append(legacy)

        if not tokens:
            raise ValueError(
                "GitHub token is required. Set GITHUB_TOKEN or GITHUB_TOKEN1-3 environment variables."
            )

        self._tokens = tokens
        self._token_cycle = cycle(self._tokens)

        # Base headers without Authorization; each request will get its own token
        self.base_headers = {
            'Accept': 'application/vnd.github.v3+json'
        }
        # Semaphore to limit concurrent downloads to GitHub Raw content
        self.download_semaphore = asyncio.Semaphore(50)
        logger.info("Initialized Async Vacuum Extractor")

    def _next_headers(self) -> Dict[str, str]:
        """Return headers with the next token in round robin."""
        token = next(self._token_cycle)
        headers = dict(self.base_headers)
        headers['Authorization'] = f'token {token}'
        return headers

    async def _fetch_tree(self, session: aiohttp.ClientSession, owner: str, repo: str, commit_sha: str) -> List[Dict]:
        """Fetch the entire repository tree for a specific commit."""
        url = f"https://api.github.com/repos/{owner}/{repo}/git/trees/{commit_sha}?recursive=1"
        # Rotate token per request to distribute across tokens
        try:
            async with session.get(url, headers=self._next_headers()) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('tree', [])
                elif response.status == 404:
                    logger.warning(f"Tree not found for {owner}/{repo}@{commit_sha}")
                    return []
                else:
                    logger.error(f"Failed to fetch tree for {owner}/{repo}@{commit_sha}: {response.status}")
                    return []
        except asyncio.TimeoutError:
            logger.warning(f"Timeout while fetching tree for {owner}/{repo}@{commit_sha}")
            return []

    async def _download_to_memory(self, session: aiohttp.ClientSession, owner: str, repo: str, commit_sha: str, path: str) -> Tuple[str, Optional[str]]:
        """Download a file directly to memory (used for YAMLs)."""
        url = f"https://raw.githubusercontent.com/{owner}/{repo}/{commit_sha}/{path}"
        async with self.download_semaphore:
            try:
                async with session.get(url, headers=self._next_headers()) as response:
                    if response.status == 200:
                        return path, await response.text()
                    return path, None
            except asyncio.TimeoutError:
                logger.warning(f"Timeout while downloading workflow file {path} for {owner}/{repo}@{commit_sha}")
                return path, None

    async def _download_to_disk(self, session: aiohttp.ClientSession, owner: str, repo: str, commit_sha: str, path: str, dest_dir: str):
        """Download a file directly to disk (used for source code)."""
        url = f"https://raw.githubusercontent.com/{owner}/{repo}/{commit_sha}/{path}"
        
        # Flatten directory structure for lizard
        safe_filename = path.replace('/', '_')
        dest_path = os.path.join(dest_dir, safe_filename)
        
        async with self.download_semaphore:
            try:
                async with session.get(url, headers=self._next_headers()) as response:
                    if response.status == 200:
                        content = await response.read()
                        # Blocking I/O inside async is generally fine for small files, 
                        # but could be offloaded to threads if files are massive.
                        with open(dest_path, 'wb') as f:
                            f.write(content)
            except asyncio.TimeoutError:
                logger.warning(f"Timeout while downloading source file {path} for {owner}/{repo}@{commit_sha}")

    async def prepare_repository(self, session: aiohttp.ClientSession, repo_full_name: str, commit_sha: str, base_temp_dir: str) -> Tuple[Dict[str, str], str]:
        """
        Fetches the tree, downloads workflow YAMLs to memory, and source files to a temp directory.
        Returns: (dict of workflow contents, path to downloaded source files)
        """
        parts = repo_full_name.strip('/').split('/')
        if len(parts) < 2:
            return {}, ""
            
        owner, repo = parts[-2], parts[-1]
        
        # 1. Fetch Tree
        tree = await self._fetch_tree(session, owner, repo, commit_sha)
        if not tree:
            return {}, ""
            
        # 2. Filter paths
        workflow_paths = []
        source_paths = []
        
        for item in tree:
            if item['type'] != 'blob':
                continue
                
            path = item['path']
            if path.startswith('.github/workflows/') and path.endswith(('.yml', '.yaml')):
                workflow_paths.append(path)
            else:
                _, ext = os.path.splitext(path)
                if ext.lower() in self.SOURCE_EXTENSIONS:
                    source_paths.append(path)

        # 3. Create temp directory for source code
        repo_temp_dir = os.path.join(base_temp_dir, f"{owner}_{repo}_{commit_sha}")
        os.makedirs(repo_temp_dir, exist_ok=True)
        
        # 4. Concurrently download files
        memory_tasks = [self._download_to_memory(session, owner, repo, commit_sha, path) for path in workflow_paths]
        disk_tasks = [self._download_to_disk(session, owner, repo, commit_sha, path, repo_temp_dir) for path in source_paths]
        
        # Wait for all downloads to complete
        memory_results = await asyncio.gather(*memory_tasks)
        if disk_tasks:
            await asyncio.gather(*disk_tasks)
            
        workflow_contents = {path: content for path, content in memory_results if content}
        
        return workflow_contents, repo_temp_dir

    # --- Structural & Parsing Methods (Unchanged core logic, pure in-memory) ---

    def _calculate_yaml_depth(self, yaml_data: Any, current_depth: int = 0) -> int:
        if not isinstance(yaml_data, dict):
            return current_depth
        max_depth = current_depth
        for value in yaml_data.values():
            if isinstance(value, dict):
                max_depth = max(max_depth, self._calculate_yaml_depth(value, current_depth + 1))
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        max_depth = max(max_depth, self._calculate_yaml_depth(item, current_depth + 1))
        return max_depth
    
    def _count_matrix_permutations(self, matrix_config: Any) -> int:
        if not isinstance(matrix_config, dict):
            return 1
        total_permutations = 1
        for key, values in matrix_config.items():
            if key in ('exclude', 'include'):
                continue
            if isinstance(values, list):
                total_permutations *= len(values)
        return max(1, total_permutations)
    
    def _extract_os_label(self, job_data: Dict) -> str:
        runs_on = job_data.get('runs-on', '')
        if isinstance(runs_on, str):
            runs_on_lower = runs_on.lower()
            if 'windows' in runs_on_lower: return 'windows'
            elif 'macos' in runs_on_lower or 'mac' in runs_on_lower: return 'macos'
            elif 'self-hosted' in runs_on_lower: return 'self-hosted'
            else: return 'ubuntu'
        elif isinstance(runs_on, list) and runs_on:
            return self._extract_os_label({'runs-on': runs_on[0]})
        return 'ubuntu'
    
    def _is_setup_action(self, action_uses: str) -> bool:
        setup_patterns = ['actions/setup-node', 'actions/setup-python', 'actions/setup-go', 'actions/setup-java', 'actions/setup-dotnet', 'actions/setup-ruby']
        return any(pattern in action_uses for pattern in setup_patterns)
    
    def _is_docker_action(self, action_uses: str) -> bool:
        docker_patterns = ['docker/build-push-action', 'docker/login-action', 'docker/setup-buildx-action', 'docker/setup-qemu-action', 'docker/metadata-action']
        return any(pattern in action_uses for pattern in docker_patterns)
    
    def _is_cache_action(self, action_uses: str) -> bool:
        return 'actions/cache' in action_uses
    
    def _count_conditions(self, data: Any) -> int:
        if isinstance(data, dict):
            count = 1 if 'if' in data else 0
            return count + sum(self._count_conditions(v) for v in data.values())
        elif isinstance(data, list):
            return sum(self._count_conditions(item) for item in data)
        return 0
    
    def _count_env_variables(self, data: Any) -> int:
        if isinstance(data, dict):
            count = len(data.get('env', {})) if isinstance(data.get('env'), dict) else 0
            return count + sum(self._count_env_variables(v) for v in data.values())
        elif isinstance(data, list):
            return sum(self._count_env_variables(item) for item in data)
        return 0
    
    def _count_needs_dependencies(self, data: Any) -> int:
        if isinstance(data, dict):
            count = 0
            if 'needs' in data:
                needs = data['needs']
                count = len(needs) if isinstance(needs, list) else 1
            return count + sum(self._count_needs_dependencies(v) for v in data.values())
        elif isinstance(data, list):
            return sum(self._count_needs_dependencies(item) for item in data)
        return 0
    
    def parse_yaml_to_vector(self, yaml_content: str) -> Optional[Dict[str, Any]]:
        """Parse YAML content and extract comprehensive feature vector directly from memory."""
        try:
            yaml_data = yaml.safe_load(yaml_content)
            if not yaml_data or not isinstance(yaml_data, dict):
                return None
            
            features = {}
            features['yaml_line_count'] = len(yaml_content.splitlines())
            features['yaml_depth'] = self._calculate_yaml_depth(yaml_data)
            
            jobs = yaml_data.get('jobs', {})
            job_count = len(jobs)
            features['job_count'] = job_count
            
            total_steps = 0
            uses_matrix_strategy = False
            matrix_dimensions = 0
            matrix_permutations = 1
            fail_fast = True
            os_labels = set()
            container_images = []
            timeout_minutes = []
            unique_actions = set()
            setup_actions_count = 0
            docker_actions_count = 0
            cache_actions_count = 0
            
            for job_name, job_config in jobs.items():
                if isinstance(job_config, dict):
                    # Steps
                    steps = job_config.get('steps', [])
                    if isinstance(steps, list):
                        total_steps += len(steps)
                        for step in steps:
                            if isinstance(step, dict) and 'uses' in step:
                                action_uses = step['uses']
                                unique_actions.add(action_uses)
                                if self._is_setup_action(action_uses): setup_actions_count += 1
                                if self._is_docker_action(action_uses): docker_actions_count += 1
                                if self._is_cache_action(action_uses): cache_actions_count += 1
                    
                    # Strategy
                    strategy = job_config.get('strategy', {})
                    if isinstance(strategy, dict) and 'matrix' in strategy:
                        uses_matrix_strategy = True
                        matrix = strategy['matrix']
                        if isinstance(matrix, dict):
                            matrix_dimensions = len([k for k in matrix.keys() if k not in ['exclude', 'include']])
                            matrix_permutations = self._count_matrix_permutations(matrix)
                        fail_fast = strategy.get('fail-fast', True)
                    
                    # Env
                    os_labels.add(self._extract_os_label(job_config))
                    container = job_config.get('container')
                    if container:
                        if isinstance(container, str): container_images.append(container)
                        elif isinstance(container, dict) and 'image' in container: container_images.append(container['image'])
                    
                    timeout = job_config.get('timeout-minutes')
                    if timeout: timeout_minutes.append(int(timeout))
            
            features['total_steps'] = total_steps
            features['avg_steps_per_job'] = total_steps / max(1, job_count)
            features['uses_matrix_strategy'] = uses_matrix_strategy
            features['matrix_dimensions'] = matrix_dimensions
            features['matrix_permutations'] = matrix_permutations
            features['fail_fast'] = fail_fast
            features['os_label'] = list(os_labels)[0] if os_labels else 'ubuntu'
            features['container_image'] = len(container_images) > 0
            features['timeout_minutes'] = sum(timeout_minutes) / len(timeout_minutes) if timeout_minutes else 0
            features['unique_actions_used'] = len(unique_actions)
            features['is_using_setup_actions'] = setup_actions_count > 0
            features['is_using_docker_actions'] = docker_actions_count > 0
            features['is_using_cache'] = cache_actions_count > 0
            
            # Logic & Flow
            features['env_var_count'] = self._count_env_variables(yaml_data)
            features['if_condition_count'] = self._count_conditions(yaml_data)
            features['needs_dependencies_count'] = self._count_needs_dependencies(yaml_data)
            
            return features
            
        except yaml.YAMLError:
            return None
        except Exception as e:
            logger.error(f"Unexpected error during feature extraction: {e}")
            return None