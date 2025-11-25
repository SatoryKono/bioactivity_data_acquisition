# PaginatorABC

**Назначение:** Определяет стратегию постраничного обхода при получении данных.

```python
from typing import Generic, TypeVar, Optional

RequestT = TypeVar("RequestT")
ResponseT = TypeVar("ResponseT")

class PaginatorABC(Generic[RequestT, ResponseT]):
    def get_next_request(self, prev_request: RequestT, last_response: ResponseT) -> Optional[RequestT]:
        """Вычислить запрос следующей страницы."""
        raise NotImplementedError
```