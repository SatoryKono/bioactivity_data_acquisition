# Как установить Bioactivity Data Acquisition

## Задача

Установить и настроить Bioactivity Data Acquisition в вашей среде разработки.

## Предпосылки

Перед выполнением убедитесь, что у вас есть:
- Python 3.11+ установлен
- pip обновлён до последней версии
- Доступ к интернету для загрузки зависимостей

## Решение

### Шаг 1: Клонирование репозитория

```bash
git clone https://github.com/SatoryKono/bioactivity_data_acquisition.git
cd bioactivity_data_acquisition
```

### Шаг 2: Создание виртуального окружения

```bash
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
# или
.venv\Scripts\activate     # Windows
```

### Шаг 3: Установка зависимостей

```bash
pip install --upgrade pip
pip install .[dev]
```

### Шаг 4: Проверка установки

```bash
bioactivity-data-acquisition --help
```

## Проверка результата

Выполните следующую команду для проверки:

```bash
bioactivity-data-acquisition version
```

**Ожидаемый результат:**
```
bioactivity-data-acquisition version 0.1.0
```

## Альтернативные способы

### Способ 1: Установка без dev-зависимостей

```bash
pip install .
```

### Способ 2: Установка в режиме разработки

```bash
pip install -e .[dev]
```

### Способ 3: Установка через Docker

```bash
docker build -t bioactivity-data-acquisition .
docker run --rm bioactivity-data-acquisition --help
```

## Troubleshooting

### Проблема: Ошибка "python: command not found"

**Симптомы:**
- Команда `python` не найдена
- Ошибка при создании виртуального окружения

**Причина:**
Python не установлен или не добавлен в PATH

**Решение:**
1. Установите Python 3.11+ с [python.org](https://python.org)
2. Убедитесь, что Python добавлен в PATH
3. Проверьте установку: `python --version`

### Проблема: Ошибка "pip: command not found"

**Симптомы:**
- Команда `pip` не найдена
- Ошибка при установке пакетов

**Причина:**
pip не установлен или не обновлён

**Решение:**
```bash
python -m ensurepip --upgrade
python -m pip install --upgrade pip
```

### Проблема: Ошибка "Permission denied"

**Симптомы:**
- Ошибки доступа при установке
- Проблемы с правами в системных директориях

**Причина:**
Попытка установки в системные директории без прав

**Решение:**
Используйте виртуальное окружение:
```bash
python -m venv .venv
source .venv/bin/activate
pip install .[dev]
```

### Проблема: Ошибка "No module named 'library'"

**Симптомы:**
- Модуль не найден после установки
- Ошибки импорта

**Причина:**
Пакет не установлен или виртуальное окружение не активировано

**Решение:**
1. Активируйте виртуальное окружение
2. Переустановите пакет:
```bash
pip uninstall bioactivity-data-acquisition
pip install .[dev]
```

## Связанные темы

- [Быстрый старт](../tutorials/quickstart.md)
- [Конфигурация](../reference/configuration/index.md)
- [Разработка](development.md)
