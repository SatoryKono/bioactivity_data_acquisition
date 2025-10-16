#!/usr/bin/env python3
"""Проверка исходного CSV файла."""

import pandas as pd

# Читаем исходный CSV
df = pd.read_csv('data/input/documents.csv')

print("=== Анализ исходного CSV файла ===")
print(f"Количество строк: {len(df)}")
print(f"Количество колонок: {len(df.columns)}")
print()

print("Колонки в исходном CSV:")
for i, col in enumerate(df.columns, 1):
    print(f"{i:2d}. {col}")
print()

print("Проверяем нужные поля:")
print(f"classification: {df['classification'].iloc[0] if 'classification' in df.columns else 'НЕТ'}")
print(f"document_contains_external_links: {df['document_contains_external_links'].iloc[0] if 'document_contains_external_links' in df.columns else 'НЕТ'}")
print(f"is_experimental_doc: {df['is_experimental_doc'].iloc[0] if 'is_experimental_doc' in df.columns else 'НЕТ'}")
print()

print("Типы данных для этих полей:")
if 'classification' in df.columns:
    print(f"classification: {df['classification'].dtype}")
if 'document_contains_external_links' in df.columns:
    print(f"document_contains_external_links: {df['document_contains_external_links'].dtype}")
if 'is_experimental_doc' in df.columns:
    print(f"is_experimental_doc: {df['is_experimental_doc'].dtype}")
print()

print("Уникальные значения в этих полях:")
if 'classification' in df.columns:
    print(f"classification: {df['classification'].unique()[:10]}")  # Первые 10 уникальных значений
if 'document_contains_external_links' in df.columns:
    print(f"document_contains_external_links: {df['document_contains_external_links'].unique()}")
if 'is_experimental_doc' in df.columns:
    print(f"is_experimental_doc: {df['is_experimental_doc'].unique()}")
