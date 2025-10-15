# Нормализация названий журналов

## Обзор

Модуль `library.tools.journal_normalizer` предоставляет функциональность для нормализации названий научных журналов с целью обеспечения единообразия и возможности точного сравнения одинаковых изданий, представленных в разных форматах.

## Основные функции

### `normalize_journal_name(text) -> str | None`

Нормализует название журнала согласно установленным правилам.

**Параметры:**
- `text` (Any): Исходное название журнала

**Возвращает:**
- Нормализованное название журнала (str) или None для пустых значений

### `normalize_journal_columns(df) -> pd.DataFrame`

Нормализует все колонки с названиями журналов в DataFrame.

**Параметры:**
- `df` (pd.DataFrame): DataFrame с данными документов

**Возвращает:**
- DataFrame с нормализованными колонками журналов

### `get_journal_columns(df) -> list[str]`

Возвращает список колонок с названиями журналов.

**Параметры:**
- `df` (pd.DataFrame): DataFrame для анализа

**Возвращает:**
- Список названий колонок, содержащих 'journal' или 'журнал' (case-insensitive)

## Правила нормализации

Нормализация применяется строго по порядку:

### 1. Trim
Обрезаются пробелы в начале и конце строки.

### 2. Unicode нормализация
- Приведение к NFC (Canonical Decomposition, followed by Canonical Composition)
- Применение casefold (нижний регистр с учётом юникода)

### 3. Декорации
- Удаление внешних кавычек (`"`, `'`)
- Удаление завершающей пунктуации (`.`, `,`, `;`, `:`, `)`, `]`, `}`)

### 4. Замена символов
- `&` и `+` заменяются на ` and `

### 5. Сокращения
- Удаление точек в сокращениях (`j. chem. phys.` → `j chem phys`)
- Замена распространенных сокращений на полные слова:
  - `j` → `journal`
  - `chem` → `chemical`
  - `phys` → `physics`
  - `biol` → `biology`
  - `math` → `mathematics`
  - `eng` → `engineering`
  - `med` → `medicine`
  - `tech` → `technology`
  - `sci` → `science`
  - `res` → `research`
  - `appl` → `applied`
  - `theor` → `theoretical`
  - `exp` → `experimental`
  - `int` → `international`
  - `proc` → `proceedings`
  - `trans` → `transactions`
  - `comm` → `communications`
  - `lett` → `letters`
  - `rev` → `reviews`
  - `rep` → `reports`
  - `bull` → `bulletin`
  - `ann` → `annals`
  - `mag` → `magazine`
  - `news` → `newsletter`
  - `not` → `notes`

### 6. Пробелы и дефисы
- Замена множественных пробелов на один пробел
- Дефисы окружаются пробелами, затем множественные пробелы схлопываются

### 7. Диакритика
- Применение NFKD (Normalization Form Canonical Decomposition)
- Удаление комбинируемых диакритических знаков (`résumé` → `resume`)

### 8. Артикли и служебные слова
Удаление в начале строки:
- `the `
- `a `
- `an `
- `le `
- `la `
- `les `
- `el `
- `los `
- `las `

### 9. Нормализация типовых слов
Стандартизация слов-классов изданий:
- `journals` → `journal` (единственное число)
- `transactions` → `transactions`
- `proceedings` → `proceedings`
- `letters` → `letters`
- `reports` → `reports`
- `bulletin` → `bulletin`
- `communications` → `communications`
- `reviews` → `reviews`
- `annals` → `annals`
- `magazine` → `magazine`
- `newsletter` → `newsletter`
- `notes` → `notes`

### 10. Предлоги и соединители
Унификация к `of`:
- `de` → `of`
- `di` → `of`
- `der` → `of`
- `für` → `of`
- `for` → `of`

### 11. Римские цифры
Конвертация в арабские:
- `i` → `1`
- `ii` → `2`
- `iii` → `3`
- `iv` → `4`
- `v` → `5`
- `vi` → `6`
- `vii` → `7`
- `viii` → `8`
- `ix` → `9`
- `x` → `10`
- и т.д. до `xx` → `20`

### 12. Итоговая форма
- Оставляются только символы `[a-z0-9]`, пробел и дефис
- Удаление всех остальных символов
- Схлопывание множественных пробелов
- Удаление пробелов вокруг дефисов
- Обрезка строки
- Пустые значения помечаются как `None`

## Примеры нормализации

| Исходное название | Нормализованное название |
|-------------------|-------------------------|
| "The Journal of Chemical Physics" | "journal of chemical physics" |
| "J. Chem. Phys." | "journal chemical physics" |
| "Proceedings of the IEEE" | "proceedings of the ieee" |
| "IEEE Transactions on Pattern Analysis" | "ieee transactions on pattern analysis" |
| "Annales de Physique" | "annales of physique" |
| "Revista de Biología" | "revista of biologia" |
| "Letters in Applied Microbiology" | "letters in applied microbiology" |
| "Bulletin-of Mathematical Biology" | "bulletin-of mathematical biology" |
| "Journal & Science" | "journal and science" |
| "Journal + Science" | "journal and science" |
| "Part II" | "part 2" |
| "  Nature  " | "nature" |
| "\"Science\"" | "science" |
| "Cell." | "cell" |

## Интеграция с pipeline

Функция автоматически вызывается при сохранении документов через `write_document_outputs()`.

Нормализация применяется к колонкам, содержащим 'journal' или 'журнал' в названии:
- `journal`
- `pubmed_journal_title`
- `chembl_journal`
- `crossref_journal`
- и другие подобные колонки

## Правило уникальности

- Одна и та же сущность журнала в разных написаниях даёт совпадающую нормализованную строку
- Разные журналы не схлопываются: не удаляются содержательные слова, не выкидывается тематика/область/серии
- Никаких ручных синонимов и «умных» переименований - только детерминированные преобразования

## Контроль качества

После нормализации в целевых колонках:
- Нет верхнего регистра
- Нет диакритики
- Нет `&` или `+`
- Нет финальной пунктуации
- Нет дублей пробелов
- Идентичные журналы дают ровно одинаковые строки, сравнимые оператором точного равенства

## Тестирование

Запустите демонстрационный скрипт:
```bash
python scripts/test_journal_normalizer_demo.py
```

## История изменений

- **2025-10-15**: Первоначальная реализация функциональности нормализации названий журналов
