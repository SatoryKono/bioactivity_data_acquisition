# HasherABC

```python
from abc import ABC, abstractmethod
from typing import TypeVar, Generic

RecordT = TypeVar('RecordT')
HashT = TypeVar('HashT')

class HasherABC(Generic[RecordT, HashT], ABC):
    """Интерфейс хеширования записей и ключей."""
    @abstractmethod
    def hash_record(self, record: RecordT) -> HashT: pass

    @abstractmethod
    def hash_key(self, record: RecordT) -> HashT: pass
```