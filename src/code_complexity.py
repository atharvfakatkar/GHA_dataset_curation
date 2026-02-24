#!/usr/bin/env python3
"""
Code Complexity Extractor
Computes normalized cyclomatic complexity (complexity density)
for a directory of downloaded source files.
"""

import lizard
import logging

logger = logging.getLogger(__name__)

class CodeComplexityExtractor:
    """
    Computes a single scalar complexity metric:
    complexity_density = total_cyclomatic_complexity / total_loc
    """

    # Since the new extractor only downloads specific source extensions, 
    # many of these exclusions are less likely to be hit, but they serve 
    # as a solid safety net against generated or minified code.
    EXCLUDED_PATTERNS = {
        "node_modules",
        "venv",
        "env",
        "build",
        "dist",
        "target",
        "__pycache__",
        "min.js",
        "min.css"
    }

    def __init__(self, target_dir: str):
        """
        Initialize the complexity extractor.
        
        Args:
            target_dir: Directory containing the downloaded source code files.
        """
        self.target_dir = target_dir

    def _is_valid_file(self, file_path: str) -> bool:
        """
        Filters unwanted directories or minified files.
        """
        for excluded in self.EXCLUDED_PATTERNS:
            if excluded in file_path:
                return False
        return True

    def extract_complexity(self) -> float:
        """
        Analyzes the target directory and returns the normalized complexity score.
        """
        total_ccn = 0
        total_loc = 0

        try:
            # lizard.analyze accepts a list of directory/file paths
            analysis = lizard.analyze([self.target_dir])

            for file_info in analysis:
                # Lizard's filename might include the path, check against exclusions
                if not self._is_valid_file(file_info.filename):
                    continue

                total_loc += file_info.nloc

                for func in file_info.function_list:
                    total_ccn += func.cyclomatic_complexity

            if total_loc == 0:
                return 0.0

            complexity_density = total_ccn / total_loc
            return round(complexity_density, 6)

        except Exception as e:
            logger.error(f"Error computing code complexity in {self.target_dir}: {e}")
            return 0.0