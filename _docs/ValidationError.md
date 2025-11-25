# ValidationError

```python
from typing import Optional

class ValidationError:
    """Описывает одну ошибку валидации данных для любого этапа пайплайна.
    Содержит название поля, сообщение об ошибке и необязательный код ошибки."""
    def __init__(self, field: Optional[str], message: str, code: Optional[str] = None):
        self.field = field
        self.message = message
        self.code = code
```