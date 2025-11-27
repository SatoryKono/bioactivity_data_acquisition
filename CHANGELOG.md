# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Removed
- Удалён модуль `bioetl.clients.transports` и реэкспорт `RequestsTransport`/`AioHttpTransport`; используйте `UnifiedAPIClient` и его адаптеры для работы с транспортом ChEMBL.
