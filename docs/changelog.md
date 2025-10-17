# Релизы и изменения

Следуем SemVer. Формат — Keep a Changelog.

## Процесс выпуска

1. Обновить `CHANGELOG` (секция Unreleased → новый тег)
2. Обновить версии в проекте (при необходимости)
3. Создать тег `vX.Y.Z` и GitHub Release
4. Проверить сборку документации и CI

## [Unreleased]

### Added

- Документация (MkDocs Material), API Reference, диаграммы, CI публикации
- Настроен Git LFS для `*.parquet`, `*.pkl`, `*.xlsm`, `*.png`, `*.jpg`
- Добавлены pre-commit хуки (порог 500 КБ, блокировка артефактов)
- CI публикует test outputs, coverage и security отчёты как artifacts

### Changed

-.gitignore расширен: OS/IDE файлы, test outputs, site/, временные файлы
- Обновлены пути конфигурации на `configs/`

### Fixed

Шаблон (Keep a Changelog):

```markdown
## [Unreleased]
### Added
### Changed
### Fixed

## [0.1.0] - 2025-10-16
### Added
- Initial release
```
