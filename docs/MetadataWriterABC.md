# MetadataWriterABC

**Назначение:** Сохраняет служебные метаданные рядом с основным результатом пайплайна.

```python
from pathlib import Path
from typing import Generic, TypeVar

MetadataT = TypeVar("MetadataT")

class MetadataWriterABC(Generic[MetadataT]):
    def write_metadata(self, target_path: Path, metadata: MetadataT) -> None:
        raise NotImplementedError
```