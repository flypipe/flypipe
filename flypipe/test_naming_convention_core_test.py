"""
Test to ensure all test files follow the naming convention.

Test files must end with one of:
- core_test.py (for backend-agnostic tests)
- pyspark_test.py (for PySpark-specific tests)
- snowpark_test.py (for Snowpark-specific tests)

Any file ending with _test.py that doesn't match one of these suffixes is invalid.
"""
import os
from pathlib import Path
import pytest


@pytest.mark.skipif(
    os.environ.get("RUN_MODE") != "CORE",
    reason="Core tests require RUN_MODE=CORE",
)
class TestNamingConventionCore:
    """Verify that all test files follow the naming convention"""

    def test_all_test_files_follow_naming_convention(self):
        """Ensure all *_test.py files end with core_test.py, pyspark_test.py, or snowpark_test.py"""
        # Get the flypipe directory (parent of this test file)
        flypipe_dir = Path(__file__).parent
        
        # Valid test file suffixes
        valid_suffixes = ["core_test.py", "pyspark_test.py", "snowpark_test.py"]
        
        # Find all Python files that look like tests
        invalid_test_files = []
        
        for root, dirs, files in os.walk(flypipe_dir):
            # Skip __pycache__ and .pytest_cache directories
            dirs[:] = [d for d in dirs if d not in ["__pycache__", ".pytest_cache", ".git"]]
            
            for file in files:
                # Check if it's a test file (ends with _test.py)
                if file.endswith("_test.py"):
                    # Check if it follows the naming convention
                    if not any(file.endswith(suffix) for suffix in valid_suffixes):
                        relative_path = os.path.relpath(
                            os.path.join(root, file), flypipe_dir
                        )
                        invalid_test_files.append(relative_path)
        
        # Assert that no invalid test files were found
        if invalid_test_files:
            error_message = (
                f"\n\nFound {len(invalid_test_files)} test file(s) that don't follow the naming convention.\n"
                f"All test files must end with one of: {', '.join(valid_suffixes)}\n\n"
                "Invalid files:\n"
            )
            for file in sorted(invalid_test_files):
                error_message += f"  - {file}\n"
            
            error_message += (
                "\nPlease rename these files to include the appropriate suffix:\n"
                "  - *_core_test.py for backend-agnostic tests\n"
                "  - *_pyspark_test.py for PySpark-specific tests\n"
                "  - *_snowpark_test.py for Snowpark-specific tests\n"
            )
            
            assert False, error_message

