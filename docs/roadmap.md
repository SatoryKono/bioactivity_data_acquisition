# Roadmap

Дорожная карта фиксирует пайплайны, которые пока не материализованы в CLI, но
имеют черновую документацию и дизайн. Активные команды перечислены в
[`docs/cli/01-cli-commands.md`](cli/01-cli-commands.md); карточки ниже служат
единственным источником правды для незавершённых направлений.

| Код/команда        | Источник данных | Статус        | Проектные ссылки |
| ------------------ | --------------- | ------------- | ---------------- |
| `pubchem`          | PubChem         | Design only   | [TestItem overview](pipelines/pubchem/testitem/00-testitem-pubchem-overview.md) |
| `uniprot`          | UniProt         | Design only   | [Target overview](pipelines/uniprot/target/00-target-uniprot-overview.md) |
| `gtp_iuphar`       | Guide to Pharmacology | Design only | [Target overview](pipelines/iuphar/target/00-target-iuphar-overview.md) |
| `openalex`         | OpenAlex        | Design only   | [Document overview](pipelines/openalex/document/00-document-openalex-overview.md) |
| `crossref`         | Crossref        | Design only   | [Document overview](pipelines/crossref/document/00-document-crossref-overview.md) |
| `pubmed`           | PubMed          | Design only   | [Document overview](pipelines/pubmed/document/00-document-pubmed-overview.md) |
| `semantic_scholar` | Semantic Scholar | Design only  | [Document overview](pipelines/semanticscholar/document/00-document-semanticscholar-overview.md) |

> Обновляя дорожную карту, переносите сюда новые направления вместо добавления
> заглушек в `COMMAND_REGISTRY` или help-тексты CLI.
