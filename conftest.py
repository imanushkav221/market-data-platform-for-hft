"""Make `python -m pytest` work without setting PYTHONPATH first.

Small thing, but the alternative is a new joiner cloning the repo, running the
tests, getting an import error, and concluding the project is broken.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))
