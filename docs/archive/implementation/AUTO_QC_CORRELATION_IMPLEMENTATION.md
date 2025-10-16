# Реализация автоматического сохранения QC и корреляционных отчетов

## Обзор

В проект добавлена функциональность автоматического сохранения таблиц корреляций
и таблиц по качеству данных при каждом сохранении итоговой таблицы с данными.
Это обеспечивает комплексный анализ качества данных и взаимосвязей между
переменными без необходимости дополнительной настройки.

## Функциональность

### Автоматическая генерация отчетов

При каждом сохранении итоговых данных через функцию `write*deterministic*csv`автоматически
создаются:

#### 1. Базовые отчеты

- **QC отчет**: Базовая оценка качества данных с основными метриками

- **Корреляционный отчет**: Базовая корреляционная матрица для числовых данных

#### 2. Расширенные QC отчеты

- **Расширенный QC отчет**: Детальные метрики качества данных

- **Детальные QC отчеты**: Разбивка по категориям (summary, patterns, top values)

#### 3. Расширенные корреляционные отчеты

- **Корреляционные матрицы**: Pearson, Spearman, Covariance

- **Категориальные корреляции**: Cramer's V для категориальных данных

- **Смешанные корреляции**: Eta-squared, Point-Biserial

- **Детальные отчеты**: JSON анализ и CSV инсайты

### Структура создаваемых файлов

Для файла данных`example*data.csv`создаются следующие отчеты:

```

example*data.csv                                    # Основной файл данных
example*data*quality*report.csv                     # Базовый QC отчет
example*data*correlation*report.csv # Базовая корреляционная матрица
example*data*quality*report*enhanced.csv            # Расширенный QC отчет
example*data*quality*report*detailed/               # Детальные QC отчеты
├── column*summary.csv
├── pattern*coverage.csv
└── top*values.csv
example*data*correlation*report*enhanced/ # Расширенные корреляционные отчеты
├── numeric*pearson.csv
├── numeric*spearman.csv
├── numeric*covariance.csv
├── categorical*cramers*v.csv
├── mixed*eta*squared.csv
├── mixed*point*biserial.csv
└── correlation*summary.csv
example*data*correlation*report*detailed/ # Детальные корреляционные отчеты
├── correlation*analysis.json
└── correlation*insights.csv

```

## Реализация

### Функция`*auto*generate*qc*and*correlation*reports`

**Файл:**`src/library/etl/load.py`

```

def *auto*generate*qc*and*correlation*reports(
    df: pd.DataFrame,
    data*path: Path,
    output: OutputSettings | None = None,
    logger: BoundLogger | None = None,
) -> None:
    """Автоматически генерирует и сохраняет QC и корреляционные отчеты."""

```

### Интеграция в пайплайн

Функция автоматически вызывается в`write*deterministic*csv`:

```

## Автоматически генерируем и сохраняем QC и корреляционные таблицы

if not df.empty:
    *auto*generate*qc*and*correlation*reports(
        df*to*write,
        destination,
        output,
        logger=logger
    )

```

### Логирование

Функция поддерживает детальное логирование:

```

logger.info("auto*qc*corr*start", qc*path=str(qc*path),
corr*path=str(corr*path))
logger.info("auto*qc*basic*saved", path=str(qc*path))
logger.info("auto*corr*basic*saved", path=str(corr*path))
logger.info("auto*qc*enhanced*saved", enhanced*path=str(enhanced*qc*path))
logger.info("auto*corr*enhanced*saved", enhanced*path=str(enhanced*corr*path))
logger.info("auto*qc*corr*complete", qc*files=[...], corr*files=[...])

```

## Типы создаваемых отчетов

### Базовые отчеты

#### QC отчет (`**quality*report.csv`)

- `metric`: Название метрики

- `value`: Значение метрики

- `threshold`: Пороговое значение

- `ratio`: Соотношение

- `status`: Статус (pass/fail)

#### Корреляционный отчет (`**correlation*report.csv`)

- Базовая корреляционная матрица для числовых данных

- Включает столбец index для ссылочной целостности

### Расширенные QC отчеты

#### Расширенный QC отчет (`**quality*report*enhanced.csv`)

Содержит 30+ метрик качества данных:

- `non*null`, `non*empty`, `empty*pct`

-`unique*cnt`, `unique*pct*of*non*empty`

-`pattern*cov*doi`, `pattern*cov*issn`, `pattern*cov*isbn`

-`pattern*cov*url`, `pattern*cov*email`

-`bool*like*cov`, `numeric*cov`

-`numeric*min`, `numeric*p50`, `numeric*p95`, `numeric*max`

-`numeric*mean`, `numeric*std`

-`date*cov`, `date*min`, `date*p50`, `date*max`

-`text*len*min`, `text*len*p50`, `text*len*p95`, `text*len*max`

-`guessed*roles`, `top*values`

#### Детальные QC отчеты (`**quality*report*detailed/`)

- `column*summary.csv`: Сводка по колонкам

- `pattern*coverage.csv`: Покрытие паттернов

- `top*values.csv`: Топ значения

### Расширенные корреляционные отчеты

#### Корреляционные матрицы (`**correlation*report*enhanced/`)

- `numeric*pearson.csv`: Корреляция Пирсона

- `numeric*spearman.csv`: Корреляция Спирмена

- `numeric*covariance.csv`: Ковариация

- `categorical*cramers*v.csv`: Cramer's V для категориальных данных

- `mixed*eta*squared.csv`: Eta-squared для смешанных данных

- `mixed*point*biserial.csv`: Point-Biserial корреляция

- `correlation*summary.csv`: Сводка корреляций

#### Детальные корреляционные отчеты (`**correlation*report*detailed/`)

- `correlation*analysis.json`: Полный анализ в JSON формате

- `correlation*insights.csv`: Человекочитаемые инсайты и рекомендации

## Обработка ошибок

Функция включает обработку ошибок:

```

try:

## Генерация отчетов

    ...
except Exception as e:
    if logger is not None:
logger.error("auto*qc*corr*error", error=str(e), error*type=type(e).**name**)

## Не прерываем основной процесс, только логируем ошибку

```

## Совместимость

### Обратная совместимость

✅ **Полная обратная совместимость**:

- Все существующие функции работают без изменений

- Не требуется модификация конфигурационных файлов

- Поддерживаются все форматы вывода (CSV, Parquet)

- Автоматическое сохранение работает прозрачно

### Условия работы

- **Непустые данные**: Отчеты создаются только для непустых DataFrame

- **Автоматичность**: Не требует дополнительной настройки

- **Формат данных**: Поддерживает любые типы данных в DataFrame

- **Размер данных**: Работает с данными любого размера

## Преимущества

### 1. Автоматичность

- Не требует вмешательства пользователя

- Интегрировано в существующий процесс сохранения

- Работает для всех типов данных

### 2. Комплексность

- Полный набор метрик качества данных

- Множественные типы корреляционного анализа

- Детальные инсайты и рекомендации

### 3. Удобство использования

- Предсказуемая структура файлов

- Логирование процесса

- Обработка ошибок без прерывания основного процесса

### 4. Масштабируемость

- Поддержка больших объемов данных

- Эффективная генерация отчетов

- Оптимизированное использование ресурсов

## Примеры использования

### Базовое использование

```

from library.etl.load import write*deterministic*csv

## Простое сохранение данных - отчеты создаются автоматически

write*deterministic*csv(df, "output/data.csv")

```

### С настройками

```

from library.config import OutputSettings, CsvFormatSettings

output*settings = OutputSettings(
    data*path=Path("output/data.csv"),
    qc*report*path=Path("output/qc.csv"),
    correlation*path=Path("output/corr.csv"),
    format="csv",
    csv=CsvFormatSettings()
)

write*deterministic*csv(df, Path("output/data.csv"), output=output*settings)

```

### Анализ созданных отчетов

```

import pandas as pd
import json

## Загрузка QC отчета

qc*report = pd.read*csv("output/data*quality*report.csv")
print("QC метрики:", qc*report)

## Загрузка расширенного QC отчета

enhanced*qc = pd.read*csv("output/data*quality*report*enhanced.csv")
print("Расширенные метрики:", enhanced*qc)

## Загрузка корреляционных инсайтов

insights =
pd.read*csv("output/data*correlation*report*detailed/correlation*insights.csv")
print("Корреляционные инсайты:", insights)

## Загрузка JSON анализа

with open("output/data*correlation*report*detailed/correlation*analysis.json",
'r') as f:
    analysis = json.load(f)
print("Полный анализ:", analysis)

```

## Заключение

Автоматическое сохранение QC и корреляционных отчетов обеспечивает комплексный
анализ качества данных и взаимосвязей между переменными без необходимости
дополнительной настройки. Функциональность полностью интегрирована в
существующий пайплайн и работает прозрачно для пользователя, предоставляя
богатый набор аналитических отчетов для каждого сохранения данных.
