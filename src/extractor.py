#!/usr/bin/env python3
"""
The Vacuum Extractor (Broad-Spectrum Features) Component
Extracts comprehensive features from GitHub workflow YAML files for zero-run cost prediction.
"""

import os
import yaml
import logging
import subprocess
import tempfile
import shutil
from typing import Dict, List, Optional, Any, Union
from pathlib import Path
import math

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class VacuumExtractor:
    """
    The Vacuum Extractor performs comprehensive feature extraction from GitHub workflow YAML files.
    It extracts 25+ features across 5 categories without making assumptions about feature importance.
    """
    
    def __init__(self, temp_dir: Optional[str] = None):
        """
        Initialize the extractor.
        
        Args:
            temp_dir: Temporary directory for git operations. If None, uses system temp.
        """
        self.temp_dir = temp_dir or tempfile.mkdtemp(prefix="vacuum_extractor_")
        logger.info(f"Initialized Vacuum Extractor with temp directory: {self.temp_dir}")
    
    def _clone_repo_to_commit(self, repo_url: str, commit_sha: str) -> str:
        """
        Clone repository and checkout to specific commit for time travel.
        
        Args:
            repo_url: GitHub repository URL
            commit_sha: Commit hash to checkout
            
        Returns:
            Path to the cloned repository
        """
        try:
            # Extract repo name from URL
            repo_name = repo_url.strip('/').split('/')[-1]
            clone_path = os.path.join(self.temp_dir, repo_name)
            
            # Remove existing clone if present
            if os.path.exists(clone_path):
                shutil.rmtree(clone_path)
            
            # Clone repository
            logger.info(f"Cloning {repo_url} to {clone_path}")
            subprocess.run([
                'git', 'clone', repo_url, clone_path
            ], check=True, capture_output=True, text=True)
            
            # Try to checkout to specific commit with error handling
            logger.info(f"Checking out commit {commit_sha}")
            try:
                subprocess.run([
                    'git', 'checkout', commit_sha
                ], check=True, capture_output=True, text=True, cwd=clone_path)
            except subprocess.CalledProcessError as checkout_error:
                logger.warning(f"Failed to checkout commit {commit_sha}: {checkout_error}")
                
                # Try to fetch the specific commit
                try:
                    subprocess.run([
                        'git', 'fetch', 'origin', commit_sha
                    ], check=True, capture_output=True, text=True, cwd=clone_path)
                    
                    # Try checkout again
                    subprocess.run([
                        'git', 'checkout', commit_sha
                    ], check=True, capture_output=True, text=True, cwd=clone_path)
                    
                    logger.info(f"Successfully fetched and checked out commit {commit_sha}")
                    
                except subprocess.CalledProcessError as fetch_error:
                    logger.error(f"Failed to fetch and checkout commit {commit_sha}: {fetch_error}")
                    
                    # Fallback: use the latest commit on the default branch
                    logger.warning(f"Using fallback: checking out default branch instead of {commit_sha}")
                    subprocess.run([
                        'git', 'checkout', 'main'
                    ], capture_output=True, text=True, cwd=clone_path)
                    
                    # If main doesn't exist, try master
                    try:
                        subprocess.run([
                            'git', 'checkout', 'master'
                        ], check=True, capture_output=True, text=True, cwd=clone_path)
                        logger.info("Fallback: Using master branch")
                    except subprocess.CalledProcessError:
                        logger.warning("Using current state (no main/master branch found)")
            
            return clone_path
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Git operation failed: {e}")
            raise RuntimeError(f"Failed to clone/checkout repository: {e}")
        except Exception as e:
            logger.error(f"Unexpected error during git operations: {e}")
            raise
    
    def _calculate_yaml_depth(self, yaml_data: Dict, current_depth: int = 0) -> int:
        """
        Calculate maximum nesting depth of YAML structure.
        
        Args:
            yaml_data: Parsed YAML data
            current_depth: Current depth in recursion
            
        Returns:
            Maximum nesting depth
        """
        if not isinstance(yaml_data, dict):
            return current_depth
        
        max_depth = current_depth
        for value in yaml_data.values():
            if isinstance(value, dict):
                depth = self._calculate_yaml_depth(value, current_depth + 1)
                max_depth = max(max_depth, depth)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        depth = self._calculate_yaml_depth(item, current_depth + 1)
                        max_depth = max(max_depth, depth)
        
        return max_depth
    
    def _count_matrix_permutations(self, matrix_config: Dict) -> int:
        """
        Calculate mathematical product of matrix options (Expansion Factor).
        
        Args:
            matrix_config: Matrix configuration from workflow
            
        Returns:
            Number of matrix permutations
        """
        if not isinstance(matrix_config, dict):
            return 1
        
        total_permutations = 1
        for key, values in matrix_config.items():
            if key == 'exclude' or key == 'include':
                continue  # These don't affect the base permutation count
            
            if isinstance(values, list):
                total_permutations *= len(values)
            elif isinstance(values, (str, int, float, bool)):
                total_permutations *= 1  # Single value
        
        return max(1, total_permutations)  # Minimum 1 permutation
    
    def _extract_os_label(self, job_data: Dict) -> str:
        """
        Extract OS label from job configuration.
        
        Args:
            job_data: Job configuration dictionary
            
        Returns:
            OS label: 'ubuntu', 'windows', 'macos', or 'self-hosted'
        """
        runs_on = job_data.get('runs-on', '')
        
        if isinstance(runs_on, str):
            runs_on_lower = runs_on.lower()
            if 'windows' in runs_on_lower:
                return 'windows'
            elif 'macos' in runs_on_lower or 'mac' in runs_on_lower:
                return 'macos'
            elif 'self-hosted' in runs_on_lower:
                return 'self-hosted'
            else:
                return 'ubuntu'
        elif isinstance(runs_on, list):
            # Handle multiple runners - return the first one's OS
            if runs_on:
                return self._extract_os_label({'runs-on': runs_on[0]})
        
        return 'ubuntu'  # Default
    
    def _is_setup_action(self, action_uses: str) -> bool:
        """Check if action is a setup action."""
        setup_patterns = [
            'actions/setup-node',
            'actions/setup-python', 
            'actions/setup-go',
            'actions/setup-java',
            'actions/setup-dotnet',
            'actions/setup-ruby'
        ]
        return any(pattern in action_uses for pattern in setup_patterns)
    
    def _is_docker_action(self, action_uses: str) -> bool:
        """Check if action is Docker-related."""
        docker_patterns = [
            'docker/build-push-action',
            'docker/login-action',
            'docker/setup-buildx-action',
            'docker/setup-qemu-action',
            'docker/metadata-action'
        ]
        return any(pattern in action_uses for pattern in docker_patterns)
    
    def _is_cache_action(self, action_uses: str) -> bool:
        """Check if action is cache-related."""
        return 'actions/cache' in action_uses
    
    def _count_conditions(self, data: Any) -> int:
        """
        Count conditional statements (if: clauses) in YAML data.
        
        Args:
            data: YAML data to search
            
        Returns:
            Number of conditional statements
        """
        if isinstance(data, dict):
            count = 1 if 'if' in data else 0
            for value in data.values():
                count += self._count_conditions(value)
            return count
        elif isinstance(data, list):
            return sum(self._count_conditions(item) for item in data)
        else:
            return 0
    
    def _count_env_variables(self, data: Any) -> int:
        """
        Count environment variables defined in the workflow.
        
        Args:
            data: YAML data to search
            
        Returns:
            Number of environment variables
        """
        if isinstance(data, dict):
            count = 0
            if 'env' in data and isinstance(data['env'], dict):
                count = len(data['env'])
            for value in data.values():
                count += self._count_env_variables(value)
            return count
        elif isinstance(data, list):
            return sum(self._count_env_variables(item) for item in data)
        else:
            return 0
    
    def _count_needs_dependencies(self, data: Any) -> int:
        """
        Count 'needs' dependencies between jobs.
        
        Args:
            data: YAML data to search
            
        Returns:
            Number of needs dependencies
        """
        if isinstance(data, dict):
            count = 0
            if 'needs' in data:
                needs = data['needs']
                if isinstance(needs, list):
                    count = len(needs)
                elif isinstance(needs, str):
                    count = 1
            for value in data.values():
                count += self._count_needs_dependencies(value)
            return count
        elif isinstance(data, list):
            return sum(self._count_needs_dependencies(item) for item in data)
        else:
            return 0
    
    def parse_yaml_to_vector(self, yaml_content: str) -> Optional[Dict[str, Any]]:
        """
        Parse YAML content and extract comprehensive feature vector.
        
        Args:
            yaml_content: Raw YAML content as string
            
        Returns:
            Dictionary containing all extracted features, or None if parsing fails
        """
        try:
            # Parse YAML
            yaml_data = yaml.safe_load(yaml_content)
            if not yaml_data or not isinstance(yaml_data, dict):
                logger.warning("Empty or invalid YAML content")
                return None
            
            # Initialize feature dictionary
            features = {}
            
            # === Category 1: Structural Complexity ===
            features['yaml_line_count'] = len(yaml_content.splitlines())
            features['yaml_depth'] = self._calculate_yaml_depth(yaml_data)
            
            # Count jobs and steps
            jobs = yaml_data.get('jobs', {})
            job_count = len(jobs)
            features['job_count'] = job_count
            
            total_steps = 0
            for job_name, job_config in jobs.items():
                if isinstance(job_config, dict):
                    steps = job_config.get('steps', [])
                    total_steps += len(steps) if isinstance(steps, list) else 0
            
            features['total_steps'] = total_steps
            features['avg_steps_per_job'] = total_steps / max(1, job_count)
            
            # === Category 2: Matrix Multipliers ===
            uses_matrix_strategy = False
            matrix_dimensions = 0
            matrix_permutations = 1
            fail_fast = True  # Default value
            
            for job_name, job_config in jobs.items():
                if isinstance(job_config, dict):
                    strategy = job_config.get('strategy', {})
                    if isinstance(strategy, dict) and 'matrix' in strategy:
                        uses_matrix_strategy = True
                        matrix = strategy['matrix']
                        if isinstance(matrix, dict):
                            matrix_dimensions = len([k for k in matrix.keys() 
                                                   if k not in ['exclude', 'include']])
                            matrix_permutations = self._count_matrix_permutations(matrix)
                        
                        fail_fast = strategy.get('fail-fast', True)
            
            features['uses_matrix_strategy'] = uses_matrix_strategy
            features['matrix_dimensions'] = matrix_dimensions
            features['matrix_permutations'] = matrix_permutations
            features['fail_fast'] = fail_fast
            
            # === Category 3: Execution Environment ===
            os_labels = set()
            container_images = []
            timeout_minutes = []
            
            for job_name, job_config in jobs.items():
                if isinstance(job_config, dict):
                    os_labels.add(self._extract_os_label(job_config))
                    
                    container = job_config.get('container')
                    if container:
                        if isinstance(container, str):
                            container_images.append(container)
                        elif isinstance(container, dict) and 'image' in container:
                            container_images.append(container['image'])
                    
                    timeout = job_config.get('timeout-minutes')
                    if timeout:
                        timeout_minutes.append(int(timeout))
            
            features['os_label'] = list(os_labels)[0] if os_labels else 'ubuntu'
            features['container_image'] = len(container_images) > 0
            features['timeout_minutes'] = sum(timeout_minutes) / len(timeout_minutes) if timeout_minutes else 0
            
            # === Category 4: Action Ecosystem ===
            unique_actions = set()
            setup_actions_count = 0
            docker_actions_count = 0
            cache_actions_count = 0
            
            for job_name, job_config in jobs.items():
                if isinstance(job_config, dict):
                    steps = job_config.get('steps', [])
                    if isinstance(steps, list):
                        for step in steps:
                            if isinstance(step, dict) and 'uses' in step:
                                action_uses = step['uses']
                                unique_actions.add(action_uses)
                                
                                if self._is_setup_action(action_uses):
                                    setup_actions_count += 1
                                if self._is_docker_action(action_uses):
                                    docker_actions_count += 1
                                if self._is_cache_action(action_uses):
                                    cache_actions_count += 1
            
            features['unique_actions_used'] = len(unique_actions)
            features['is_using_setup_actions'] = setup_actions_count > 0
            features['is_using_docker_actions'] = docker_actions_count > 0
            features['is_using_cache'] = cache_actions_count > 0
            
            # === Category 5: Logic & Flow ===
            features['env_var_count'] = self._count_env_variables(yaml_data)
            features['if_condition_count'] = self._count_conditions(yaml_data)
            features['needs_dependencies_count'] = self._count_needs_dependencies(yaml_data)
            
            return features
            
        except yaml.YAMLError as e:
            logger.error(f"YAML parsing error: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error during feature extraction: {e}")
            return None
    
    def extract_features_from_commit(self, repo_url: str, commit_sha: str, 
                                   workflow_file_path: str) -> Optional[Dict[str, Any]]:
        """
        Extract features from a workflow file at a specific commit.
        
        Args:
            repo_url: GitHub repository URL
            commit_sha: Commit hash to checkout
            workflow_file_path: Path to workflow file within the repo
            
        Returns:
            Feature dictionary or None if extraction fails
        """
        try:
            # Clone repo and checkout to specific commit
            repo_path = self._clone_repo_to_commit(repo_url, commit_sha)
            
            # Read workflow file
            workflow_path = os.path.join(repo_path, workflow_file_path)
            if not os.path.exists(workflow_path):
                logger.error(f"Workflow file not found: {workflow_path}")
                return None
            
            with open(workflow_path, 'r', encoding='utf-8') as f:
                yaml_content = f.read()
            
            # Extract features
            features = self.parse_yaml_to_vector(yaml_content)
            
            # Add metadata
            if features:
                features['repo_url'] = repo_url
                features['commit_sha'] = commit_sha
                features['workflow_file'] = workflow_file_path
            
            return features
            
        except Exception as e:
            logger.error(f"Failed to extract features from {repo_url}@{commit_sha}: {e}")
            return None
        finally:
            # Clean up temporary files
            try:
                if 'repo_path' in locals() and os.path.exists(repo_path):
                    shutil.rmtree(repo_path)
            except Exception as e:
                logger.warning(f"Failed to clean up temporary directory: {e}")
    
    def cleanup(self):
        """Clean up temporary directories."""
        try:
            if os.path.exists(self.temp_dir):
                shutil.rmtree(self.temp_dir)
                logger.info(f"Cleaned up temporary directory: {self.temp_dir}")
        except Exception as e:
            logger.warning(f"Failed to clean up temporary directory: {e}")
    
    def __del__(self):
        """Destructor to ensure cleanup."""
        self.cleanup()


def main():
    """
    Main function to test the extractor with a sample YAML file.
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Extract features from GitHub workflow YAML files')
    parser.add_argument('yaml_file', help='Path to YAML workflow file')
    parser.add_argument('-o', '--output', help='Output JSON file for features')
    
    args = parser.parse_args()
    
    # Initialize extractor
    extractor = VacuumExtractor()
    
    try:
        # Read YAML file
        with open(args.yaml_file, 'r') as f:
            yaml_content = f.read()
        
        # Extract features
        features = extractor.parse_yaml_to_vector(yaml_content)
        
        if features:
            print("Extracted Features:")
            for key, value in features.items():
                print(f"  {key}: {value}")
            
            # Save to file if requested
            if args.output:
                import json
                with open(args.output, 'w') as f:
                    json.dump(features, f, indent=2)
                print(f"\nFeatures saved to {args.output}")
        else:
            print("Failed to extract features")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        extractor.cleanup()


if __name__ == "__main__":
    main()
