# TESTS_SUITE

## Базовые
1) /run-assay-chembl → 6 блоков, стандартные флаги, инварианты.
2) /enforce-io-determinism → порядок колонок/строк, ISO-UTC, temp→replace, тест стабильности.
3) /pipeline-skeleton → перечень модулей, тесты, пример конфига.

## Сложные
1) /review-pipeline для target → пагинация/merge/логи, дифы патчей.
2) /benchmark-pagination offset vs cursor limit=5000 → p95/throughput/error rate, рекомендация.
3) /diff-artifacts left=A.csv right=B.csv keys=[chembl_id,assay_id] → отчёт added/removed/changed.

## Стресс
1) /run-document-chembl --mode all --limit 1e6 → рекомендации по потоковой обработке/памяти.
2) /profile-pipeline → flamegraph и top-3 bottlenecks.

## Некорректный ввод
/run-assay-chembl без --output-dir → чёткая ошибка и пример корректного вызова.
