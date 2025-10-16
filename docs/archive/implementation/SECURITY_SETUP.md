# Настройка безопасности - Краткое руководство

## Что было добавлено

✅ **Safety** - проверка уязвимостей в зависимостях  
✅ **Bandit** - статический анализ безопасности кода  
✅ **Dependabot** - автоматические обновления зависимостей  
✅ **GitHub Actions** - интеграция security checks в CI/CD  
✅ **Pre-commit hooks** - автоматические проверки при коммитах  

## Быстрый старт

### 1. Установка security tools

```bash
# Установить security tools
make security-install

# Или напрямую
pip install safety bandit
```

### 2. Запуск проверок

```bash
# Локальная проверка безопасности
make security-check

# Проверка в CI окружении
make ci-security

# Генерация отчетов
make security-report
```

### 3. Настройка pre-commit

```bash
# Установить pre-commit hooks (включая security checks)
pre-commit install
```

## Файлы конфигурации

- `.safety_policy.yaml` - политика для Safety
- `.bandit` - конфигурация Bandit
- `.banditignore` - исключения для Bandit
- `.github/dependabot.yml` - настройки Dependabot
- `.github/workflows/ci.yaml` - CI/CD pipeline

## GitHub Actions

Security checks автоматически выполняются при:
- Push в main/develop ветки
- Pull requests в main/develop ветки

## Dependabot

Автоматически создает PR для обновления:
- Python зависимостей (еженедельно)
- GitHub Actions (еженедельно)
- Docker образов (еженедельно)

## Следующие шаги

1. **Проверьте текущее состояние:**
   ```bash
   make security-check
   ```

2. **Настройте Dependabot:**
   - Укажите правильных reviewers в `.github/dependabot.yml`
   - Замените "bioactivity-team" на реальные GitHub usernames

3. **Проверьте CI pipeline:**
   - Убедитесь, что GitHub Actions работают корректно
   - Проверьте настройки репозитория

4. **Документация:**
   - Подробная документация: `docs/security.md`
   - Интеграция с архитектурой: обновите `docs/architecture.md`

## Важные заметки

- Security checks не блокируют CI при обнаружении проблем (используется `|| true`)
- Отчеты сохраняются как артефакты в GitHub Actions
- Bandit настроен на высокий уровень уверенности для уменьшения ложных срабатываний
- Safety проверяет как production, так и development зависимости
