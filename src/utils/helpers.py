import os
from datetime import datetime

def ensure_dir(path: str):
    """Create directory if not exists."""
    os.makedirs(path, exist_ok=True)

def get_ts():
    """Return timestamp string."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")
