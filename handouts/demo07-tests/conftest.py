from __future__ import annotations

import sys
from pathlib import Path

HANDOUTS_DIR = Path(__file__).resolve().parents[1]
if str(HANDOUTS_DIR) not in sys.path:
    sys.path.insert(0, str(HANDOUTS_DIR))
