# Схемы данных и валидация

Схемы описаны в Pandera для сырья и нормализованных данных.

## Входные документы (DocumentInputSchema)

| поле | тип | nullable | описание |
|---|---|---|---|
| document_chembl_id | str | no | ChEMBL document identifier |
| title | str | no | Document title |
| doi | str | yes | DOI |
| document_pubmed_id | str | yes | PubMed ID |
| chembl_doc_type | str | yes | Тип документа (ChEMBL) |
| journal | str | yes | Журнал |
| year | int | yes | Год |
| abstract | str | yes | Аннотация |
| pubmed_authors | str | yes | Авторы (PubMed) |
| document_classification | float | yes | Классификация |
| referenses_on_previous_experiments | bool | yes | Внешние ссылки |
| first_page | int | yes | Первая страница |
| original_experimental_document | bool | yes | Экспериментальный |
| issue | int | yes | Номер выпуска |
| last_page | float | yes | Последняя страница |
| month | int | yes | Месяц |
| volume | float | yes | Том |

## Выходные документы (DocumentOutputSchema, фрагмент)

| поле | тип | nullable | описание |
|---|---|---|---|
| document_chembl_id | str | no | ChEMBL ID |
| title | str | no | Title |
| doi | str | yes | DOI |
| document_pubmed_id | str | yes | PubMed ID |
| journal | str | yes | Journal |
| year | int | yes | Year |
| document_citation | str | yes | Форматированная ссылка |
| valid_doi | str | yes | Валидный DOI |
| invalid_doi | bool | yes | Флаг невалидности DOI |
| valid_journal | str | yes | Валидный журнал |
| invalid_journal | bool | yes | Флаг невалидности журнала |
| valid_year | int | yes | Валидный год |
| invalid_year | bool | yes | Флаг невалидности года |

Полный список полей — см. исходники в `src/library/schemas/document_output_schema.py`.

## Примеры

```csv
id,assay_id,value,unit
A1,AS123,1.23,uM
A2,AS124,not_a_number,uM  # невалид
```

Ошибки валидации Pandera содержат название поля и ожидаемый тип/инвариант. Примеры см. тесты.
