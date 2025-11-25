# ProgressReporterABC

**Назначение:** Интерфейс сбора статистики о ходе выполнения пайплайна.

```python
from abc import ABC, abstractmethod

class ProgressReporterABC(ABC):
    @abstractmethod
    def report_extracted(self, count: int) -> None:
        ...

    @abstractmethod
    def report_valid(self, count: int) -> None:
        ...

    @abstractmethod
    def report_discarded(self, count: int, reason: str) -> None:
        ...
```