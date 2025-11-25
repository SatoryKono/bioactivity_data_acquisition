# WriterABC

**Назначение:** Пишет коллекцию записей в хранилище по заданному пути.

```python
from pathlib import Path
from typing import Generic, TypeVar, Iterable

RecordT = TypeVar("RecordT")

class WriterABC(Generic[RecordT]):
    def write(self, records: Iterable[RecordT], output_path: Path) -> None:
        raise NotImplementedError
```