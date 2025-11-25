# ValidationResult

```python
from typing import Optional, Sequence, Any

class ValidationResult:
    """Результат валидации одной записи данных."""
    def __init__(self, record: Optional[Any], errors: Sequence[ValidationError]):
        self.record = record
        self.errors = tuple(errors)

    def is_valid(self) -> bool:
        return len(self.errors) == 0
```