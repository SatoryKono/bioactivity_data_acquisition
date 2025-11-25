# RateLimiterABC

**Назначение:** Интерфейс для локального ограничения частоты запросов и параллелизма.

```python
class RateLimiterABC:
    def acquire(self) -> None:
        """Заблокировать выполнение до получения слота для операции."""
        raise NotImplementedError

    def release(self) -> None:
        """Освободить ранее занятый слот."""
        raise NotImplementedError
```