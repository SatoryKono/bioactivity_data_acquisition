# SideInputProviderABC

**Назначение:** Загружает внешние справочники и предоставляет их как mapping.

```python
from typing import Generic, TypeVar, Mapping

SideKeyT = TypeVar("SideKeyT")
SideValueT = TypeVar("SideValueT")

class SideInputProviderABC(Generic[SideKeyT, SideValueT]):
    def load_inputs(self) -> Mapping[SideKeyT, SideValueT]:
        raise NotImplementedError
```