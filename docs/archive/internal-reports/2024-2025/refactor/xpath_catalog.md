# XPath каталог для PubMed E-utilities XML

## ArticleId (DOI)
- **XPath:** `.//ArticleId[@IdType="doi"]/text()`
- **Описание:** Извлекает DOI статьи
- **Фолбэк:** Пустая строка

## Abstract
- **XPath:** `.//AbstractText/text()`
- **Описание:** Все части abstract (может быть структурированным)
- **Объединение:** Через пробел
- **Фолбэк:** Пустая строка

## MeSH Descriptors
- **XPath:** `.//MeshHeading/DescriptorName/text()`
- **Описание:** MeSH термины-дескрипторы
- **Разделитель:** `; `
- **Фолбэк:** `"unknown"`

## MeSH Qualifiers
- **XPath:** `.//MeshHeading/QualifierName/text()`
- **Описание:** MeSH квалификаторы
- **Разделитель:** `; `
- **Фолбэк:** `"unknown"`

## Chemical List
- **XPath:** `.//Chemical/NameOfSubstance/text()`
- **Описание:** Список химических веществ
- **Разделитель:** `; `
- **Фолбэк:** `"unknown"`

## Journal Title
- **XPath:** `.//Journal/Title/text()`
- **Описание:** Название журнала
- **Фолбэк:** `"unknown"`

## Publication Date
- **XPath:** `.//PubDate/Year/text()`
- **Описание:** Год публикации
- **Фолбэк:** `"unknown"`

## Article Title
- **XPath:** `.//ArticleTitle/text()`
- **Описание:** Заголовок статьи
- **Фолбэк:** `"unknown"`

## Authors
- **XPath:** `.//AuthorList/Author/LastName/text()`
- **Описание:** Фамилии авторов
- **Разделитель:** `; `
- **Фолбэк:** `"unknown"`

## Keywords
- **XPath:** `.//Keyword/text()`
- **Описание:** Ключевые слова
- **Разделитель:** `; `
- **Фолбэк:** `"unknown"`
