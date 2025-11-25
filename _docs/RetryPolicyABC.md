# RetryPolicyABC

**Назначение:** Интерфейс стратегии повторных попыток при ошибках транспорта.

```python
from typing import Protocol

class RetryPolicyABC(Protocol):
    def should_retry(self, attempt: int, error: Exception) -> bool:
        ...

    def get_backoff_seconds(self, attempt: int) -> float:
        ...
```