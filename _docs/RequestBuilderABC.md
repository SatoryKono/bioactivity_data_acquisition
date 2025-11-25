# RequestBuilderABC

**Назначение:** Интерфейс построения транспортных запросов к источникам данных.

```python
from abc import ABC, abstractmethod
from typing import Generic, TypeVar, Optional

RequestT = TypeVar("RequestT")

class RequestBuilderABC(Generic[RequestT], ABC):
    @abstractmethod
    def build_initial(self) -> RequestT:
        ...

    @abstractmethod
    def build_for_page(self, cursor: Optional[str]) -> RequestT:
        ...
```