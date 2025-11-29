
import traceback
import sys

try:
    from bioetl.pipelines.chembl import thin
    print("Imported thin")
except ImportError:
    traceback.print_exc()
except Exception:
    traceback.print_exc()
