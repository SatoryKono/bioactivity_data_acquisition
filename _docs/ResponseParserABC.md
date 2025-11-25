# ResponseParserABC

**Назначение:** Интерфейс для разбора транспортных ответов во входные записи и курсоры.

```python
from abc import ABC, abstractmethod
from typing import Generic, TypeVar, Iterable, Optional

ResponseT = TypeVar("ResponseT")
ParsedItemT = TypeVar("ParsedItemT")

class ResponseParserABC(Generic[ResponseT, ParsedItemT], ABC):
    @abstractmethod
    def parse_items(self, response: ResponseT) -> Iterable[ParsedItemT]:
        ...

    @abstractmethod
    def extract_cursor(self, response: ResponseT) -> Optional[str]:
        ...
```