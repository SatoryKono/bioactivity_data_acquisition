# SchemaProviderABC

**Назначение:** Поставляет объект схемы данных SchemaT, используемый для валидации.

```python
from typing import Generic, TypeVar

SchemaT = TypeVar("SchemaT")

class SchemaProviderABC(Generic[SchemaT]):
    def get_schema(self) -> SchemaT:
        """Вернуть схему для валидации."""
        raise NotImplementedError
```