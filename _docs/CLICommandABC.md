# CLICommandABC

**Назначение:** Интерфейс для плагинной команды CLI, связанной с пайплайнами.

```python
from abc import ABC, abstractmethod

class CLICommandABC(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        ...

    @abstractmethod
    def run(self, args: list[str]) -> int:
        ...
```