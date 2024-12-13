"""Test configuration and shared fixtures."""

import sys
from pathlib import Path

# Add src directory to Python path for imports
src_path = str(Path(__file__).parent.parent / "src")
if src_path not in sys.path:
    sys.path.append(src_path) 