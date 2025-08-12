#!/usr/bin/env python3
"""
Pytest-based test runner for SyncTool.
"""
import sys
import os

# Ensure project root is on the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pytest


def main() -> int:
    """Run pytest with any CLI arguments passed to this script.

    Falls back to configuration in `pytest.ini`.
    """
    # Pass through any arguments given to this script to pytest
    return pytest.main(sys.argv[1:])


if __name__ == "__main__":
    sys.exit(main())
