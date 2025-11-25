# BusinessKeyDeriverABC

**Назначение:** Вычисляет стабильный бизнес-ключ записи для дедупликации и слияния.

```python
from typing import Generic, TypeVar, Hashable

RecordT = TypeVar("RecordT")
BusinessKeyT = TypeVar("BusinessKeyT", bound=Hashable)

class BusinessKeyDeriverABC(Generic[RecordT, BusinessKeyT]):
    def derive_key(self, record: RecordT) -> BusinessKeyT:
        raise NotImplementedError
```