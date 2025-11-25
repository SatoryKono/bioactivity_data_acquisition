# DQRuleABC

**Назначение:** Правило контроля качества данных. Анализирует набор записей и возвращает DQIssue.

```python
from typing import Generic, TypeVar, Iterable

RecordT = TypeVar("RecordT")

class DQRuleABC(Generic[RecordT]):
    def evaluate(self, records: Iterable[RecordT]) -> Iterable["DQIssue"]:
        raise NotImplementedError
```