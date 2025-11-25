# DQIssue

```python
class DQIssue:
    """Агрегированная проблема качества данных."""
    def __init__(self, rule: str, description: str, affected_count: int):
        self.rule = rule
        self.description = description
        self.affected_count = affected_count
```