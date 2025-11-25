# CacheABC[CacheKeyT, CacheValueT]

```python
from abc import ABC, abstractmethod
from typing import Optional, TypeVar, Generic

CacheKeyT = TypeVar('CacheKeyT')
CacheValueT = TypeVar('CacheValueT')

class CacheABC(Generic[CacheKeyT, CacheValueT], ABC):
    """Абстрактный кэш."""
    @abstractmethod
    def get(self, key: CacheKeyT) -> Optional[CacheValueT]: pass

    @abstractmethod
    def set(self, key: CacheKeyT, value: CacheValueT) -> None: pass

    @abstractmethod
    def invalidate(self, key: CacheKeyT) -> None: pass
```
