# DeduplicatorABC

**Назначение:** Удаляет дубликаты из потока записей по бизнес-ключу.

```python
from typing import Generic, TypeVar, Iterable, Callable

RecordT = TypeVar("RecordT")
BusinessKeyT = TypeVar("BusinessKeyT")

class DeduplicatorABC(Generic[RecordT, BusinessKeyT]):
    def deduplicate(self, records: Iterable[RecordT], key_fn: Callable[[RecordT], BusinessKeyT]) -> Iterable[RecordT]:
        raise NotImplementedError
```