# LookupEnricherABC

**Назначение:** Обогащает запись на основе внешнего словаря (side inputs).

```python
from typing import Generic, TypeVar, Mapping

RecordT = TypeVar("RecordT")
EnrichedRecordT = TypeVar("EnrichedRecordT")
SideKeyT = TypeVar("SideKeyT")
SideValueT = TypeVar("SideValueT")

class LookupEnricherABC(Generic[RecordT, EnrichedRecordT, SideKeyT, SideValueT]):
    def enrich(self, record: RecordT, side_inputs: Mapping[SideKeyT, SideValueT]) -> EnrichedRecordT:
        raise NotImplementedError
```