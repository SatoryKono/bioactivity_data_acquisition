# SourceClientABC

**Назначение:** Интерфейс клиента для общения с внешним источником данных.

```python
from abc import ABC, abstractmethod
from typing import Generic, TypeVar

RequestT = TypeVar("RequestT")
ResponseT = TypeVar("ResponseT")

class SourceClientABC(Generic[RequestT, ResponseT], ABC):
    @abstractmethod
    def send(self, request: RequestT) -> ResponseT:
        ...
```