# SecretProviderABC

```python
from abc import ABC, abstractmethod

class SecretProviderABC(ABC):
    """Доступ к секретным данным."""
    @abstractmethod
    def get_secret(self, name: str) -> str: pass
```