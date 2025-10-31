Аннотация: Реестр рисков по рефакторингу, вероятность/impact и mitigation. Ветка: test_refactoring_32. Дата: 2025-10-31.

| risk | source | probability | impact | mitigation | safety_tests |
|---|---|---|---|---|---|
| Регресс обогащения UniProt при слиянии сервисов | DUP-001/Batch 2 | Medium | High | Интеграционные фикстуры, сравнение до/после, golden‑сеты | Golden на 100 записей, property‑based на маппинг |
| Нарушение CLI скриптов из‑за атомарной записи | Batch 4 | Low | Medium | Бэкап старого пути, dry‑run, прогон e2e | Golden‑тесты на MD/JSON отчёты |
| Тайминги Retry‑After при переносе сетевых вызовов | Batch 3 | Medium | Medium | Тесты с фиктивным `Retry-After`, каппинг ожидания | Контракт‑тесты backoff |
| Пагинация сломает редкие edge‑cases | Batch 5 | Medium | Medium | Property‑based тестирование генераторов пагинации | Hypothesis‑тесты |
| Кэш activity повреждается при сбое записи | ARCH I/O | Low | High | Атомарная запись + temp файлы, валидация JSON | Тест на восстановление после сбоя |


