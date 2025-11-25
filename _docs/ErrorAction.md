# ErrorAction

```python
from enum import Enum

class ErrorAction(Enum):
    """Перечисление действий при возникновении ошибки в пайплайне."""
    RETRY = 1
    SKIP = 2
    ABORT = 3
```