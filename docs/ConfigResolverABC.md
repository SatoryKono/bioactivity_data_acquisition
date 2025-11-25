# ConfigResolverABC

```python
from abc import ABC, abstractmethod
from typing import Any, Mapping, TypeVar, Generic

ConfigT = TypeVar('ConfigT')

class ConfigResolverABC(Generic[ConfigT], ABC):
    """Загрузка и объединение конфигурации."""
    @abstractmethod
    def resolve(self, profile: str, overrides: Mapping[str, Any]) -> ConfigT: pass
```