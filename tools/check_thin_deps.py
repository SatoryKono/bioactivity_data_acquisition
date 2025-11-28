
import traceback

print("Checking imports for thin.py dependencies...")

try:
    from bioetl.pipelines.chembl import base
    print("OK: bioetl.pipelines.chembl.base")
except ImportError:
    print("FAIL: bioetl.pipelines.chembl.base")
    traceback.print_exc()

try:
    from bioetl.core.schemas import activity_schema
    print("OK: bioetl.core.schemas.activity_schema")
except ImportError:
    print("FAIL: bioetl.core.schemas.activity_schema")
    traceback.print_exc()

try:
    from bioetl.core.schemas import assay_schema
    print("OK: bioetl.core.schemas.assay_schema")
except ImportError:
    print("FAIL: bioetl.core.schemas.assay_schema")
    traceback.print_exc()

try:
    from bioetl.core.schemas import document_schema
    print("OK: bioetl.core.schemas.document_schema")
except ImportError:
    print("FAIL: bioetl.core.schemas.document_schema")
    traceback.print_exc()

try:
    from bioetl.core.schemas import target_schema
    print("OK: bioetl.core.schemas.target_schema")
except ImportError:
    print("FAIL: bioetl.core.schemas.target_schema")
    traceback.print_exc()

try:
    from bioetl.core.schemas import testitem_schema
    print("OK: bioetl.core.schemas.testitem_schema")
except ImportError:
    print("FAIL: bioetl.core.schemas.testitem_schema")
    traceback.print_exc()
