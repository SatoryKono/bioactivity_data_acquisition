#!/bin/bash
# Скрипт для запуска тестов

set -e

echo "🔍 Running tests..."
python -m pytest tests/ -v

echo "✅ Tests completed!"

