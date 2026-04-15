import sys
from pathlib import Path

# Allow importing project-root modules (e.g. zx_importer) from the tests/ subfolder.
sys.path.insert(0, str(Path(__file__).parent.parent))
