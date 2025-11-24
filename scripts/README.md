# scripts

Служебные утилиты BioETL запускаются через `python scripts/<name>.py`.
Каталог зафиксирован для централизации dev-скриптов и отделения их от
боевого CLI (`python -m bioetl.cli.cli_app ...`). Полный перечень команд,
опций и артефактов задокументирован в `docs/cli/03-cli-utilities.md`.

Все сценарии делегируют бизнес-логику в `bioetl.devtools.*` и ведут
журнал в соответствии с `bioetl.core.runtime.cli_errors`. История миграции
из прежнего пространства `bioetl.cli.tools` зафиксирована в
`artifacts/cli_tools_migration.csv`.
