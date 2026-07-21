"""
================================================================================
conftest.py - PYTEST CONFIGURATION  (annotated tutorial copy)
================================================================================

WHAT conftest.py IS
    A special filename pytest looks for automatically. Any conftest.py in a test
    directory is imported BEFORE the tests in that directory run, without being
    imported by name. It is the standard place for shared fixtures and setup.

WHAT THIS PARTICULAR ONE DOES
    Solves a mundane but universal problem: the tests live in demo04-tests/,
    while the code under test (demo04_common.py and friends) lives one level up
    in the parent directory. Python would not find those modules on import.

    So this prepends the PARENT directory to sys.path, letting the tests write
    `from demo04_common import ...` as if they were siblings of that file.

        parents[1]  ->  one level up from this file's directory
        sys.path    ->  the list of directories Python searches for imports
        insert(0)   ->  put it FIRST, so it wins over any same-named module
                        installed elsewhere

    The `if str(ROOT) not in sys.path` guard makes it idempotent: if pytest
    imports this file more than once, the path is not added twice.
================================================================================
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
