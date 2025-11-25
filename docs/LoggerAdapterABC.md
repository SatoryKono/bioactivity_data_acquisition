# LoggerAdapterABC

```python
from abc import ABC, abstractmethod
from typing import Any

class LoggerAdapterABC(ABC):
    """Интерфейс структурированного логгера."""
    @abstractmethod
    def info(self, message: str, **fields: Any) -> None: pass

    @abstractmethod
    def warning(self, message: str, **fields: Any) -> None: pass

    @abstractmethod
    def error(self, message: str, **fields: Any) -> None: pass
```