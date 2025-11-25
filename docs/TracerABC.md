# TracerABC

```python
from abc import ABC, abstractmethod

class TracerABC(ABC):
    """Интерфейс трассировки метрик."""
    @abstractmethod
    def record_timing(self, name: str, duration: float) -> None: pass

    @abstractmethod
    def record_counter(self, name: str, count: int = 1) -> None: pass
```