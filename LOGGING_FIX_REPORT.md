# Отчёт об исправлении ошибки логирования

**Дата:** 2025-10-20  
**Проблема:** ValueError: unsupported format character 'A' (0x41) at index 306 в urllib3 логировании

## Описание проблемы

При выполнении скрипта `get_target_data.py` возникала ошибка форматирования в системе логирования:

```
ValueError: unsupported format character 'A' (0x41) at index 306
```

Ошибка возникала в urllib3 при попытке залогировать сообщения о повторных попытках подключения, содержащие символы '%' в URL, которые интерпретировались как форматирование.

## Решение

Добавлена настройка urllib3 логирования в `src/library/logging_setup.py`:

```python
# Configure urllib3 logging to avoid format errors
urllib3_logger = logging.getLogger("urllib3.connectionpool")
urllib3_logger.setLevel(logging.ERROR)  # Only show errors, not warnings
```

Настройка добавлена в обе функции конфигурации:
- `configure_logging()` (строка 247-248)
- `_configure_programmatic_logging()` (строка 296-298)

## Результат

- ✅ Ошибка форматирования urllib3 устранена
- ✅ Логирование работает корректно
- ✅ Предупреждения urllib3 отфильтрованы (показываются только ошибки)
- ✅ Основная функциональность не нарушена

## Тестирование

Проверено:
1. Импорт и инициализация логирования работает
2. urllib3 логгер настроен на уровень ERROR
3. Ошибки сети больше не вызывают ошибки форматирования логов

## Файлы изменены

- `src/library/logging_setup.py` - добавлена настройка urllib3 логирования
