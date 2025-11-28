
import sys
import os

try:
    import bioetl
    print(f"bioetl: {bioetl.__file__}")
except ImportError:
    print("bioetl not found")

try:
    import bioetl.core
    print(f"bioetl.core: {bioetl.core.__file__}")
except ImportError:
    print("bioetl.core not found")
    
try:
    import bioetl.core.schemas
    print(f"bioetl.core.schemas: {bioetl.core.schemas.__file__}")
except ImportError as e:
    print(f"Failed to import bioetl.core.schemas: {e}")
