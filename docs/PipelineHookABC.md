# PipelineHookABC

**Назначение:** Интерфейс хуков наблюдения за выполнением пайплайна. Получает уведомления о старте/завершении пайплайна и стадий, а также об ошибках.

```python
from abc import ABC, abstractmethod

class PipelineHookABC(ABC):
    @abstractmethod
    def on_pipeline_start(self, pipeline: "PipelineBase") -> None:
        ...

    @abstractmethod
    def on_pipeline_end(self, pipeline: "PipelineBase", result: RunResult) -> None:
        ...

    @abstractmethod
    def on_stage_start(self, stage_name: str) -> None:
        ...

    @abstractmethod
    def on_stage_end(self, stage_name: str) -> None:
        ...

    @abstractmethod
    def on_stage_error(self, stage_name: str, error: Exception) -> None:
        ...

    @abstractmethod
    def on_pipeline_error(self, error: Exception) -> None:
        ...
```