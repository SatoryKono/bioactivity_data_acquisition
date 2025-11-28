# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Changed
- Конфигурация обогатителей унифицирована: ``EnricherApiConfig`` оставляет только настройки пагинации, а таймауты/ретраи
  задаются через ``EnricherClientOptions``; верхнеуровневые поля конфигурации для этих опций больше не поддерживаются.

### Removed
- Удалён модуль `bioetl.clients.transports` и реэкспорт `RequestsTransport`/`AioHttpTransport`; используйте `UnifiedAPIClient` и его адаптеры для работы с транспортом ChEMBL.
