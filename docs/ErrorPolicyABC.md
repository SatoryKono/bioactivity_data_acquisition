# ErrorPolicyABC

**Назначение:** Определяет стратегию реакции на ошибки в пайплайне. Принимает исключение и контекст, возвращает `ErrorAction`.

```python
from abc import ABC, abstractmethod
from typing import Mapping, Any

class ErrorPolicyABC(ABC):
    @abstractmethod
    def decide(self, exc: Exception, context: Mapping[str, Any]) -> ErrorAction:
        ...
```