
from types import SimpleNamespace
from typing import Mapping, Any
from enum import Enum

class ClientNamespace(str, Enum):
    CHEMBL = "chembl"

class ChemblEntity(Enum):
    ACTIVITY = "activity"

class _MappingClientContext:
    def __init__(self, mapping: Mapping[str, object]) -> None:
        self._mapping = mapping

    def get_client(self, name: str, entity: object | None = None) -> object:
        print(f"DEBUG: get_client called with name={name!r} ({type(name)}), entity={entity!r}")
        print(f"DEBUG: mapping keys: {list(self._mapping.keys())}")
        try:
            obj = self._mapping[name]
        except KeyError:
            print(f"DEBUG: KeyError for {name!r}")
            raise
        
        if entity is not None and isinstance(obj, Mapping):
            return obj[entity]
        return obj

client_primary = SimpleNamespace(process=lambda x: print(f"Process called with {x}"))
clients = {
    ClientNamespace.CHEMBL: {
        ChemblEntity.ACTIVITY: client_primary,
    }
}

client_mapping = {}
for name, client in clients.items():
    client_mapping[name] = client

ctx = _MappingClientContext(client_mapping)

try:
    client = ctx.get_client(ClientNamespace.CHEMBL, ChemblEntity.ACTIVITY)
    client.process(5)
except Exception as e:
    print(f"Caught exception: {e}")
