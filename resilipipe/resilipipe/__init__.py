import os
from pathlib import Path

__version__ = "0.1.0"

RESILIPIPE_DIR = Path(os.getenv('RESILIPIPE_DIR', Path(__file__).resolve().parent))
PROJECT_DIR = RESILIPIPE_DIR.parent.parent.parent
RESOURCES_DIR = RESILIPIPE_DIR.parent.parent / "resources"

if __name__ == "__main__":
    print(PROJECT_DIR)
