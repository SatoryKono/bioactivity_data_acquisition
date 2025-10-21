# Docker

Документация по Docker и Docker Compose для запуска пайплайнов в контейнерах.

## Dockerfile

### Мультистейдж сборка

Dockerfile использует мультистейдж сборку для оптимизации размера и безопасности:

```dockerfile
# Stage 1: Base image with Python and system dependencies
FROM python:3.11-slim as base

# Stage 2: Development image
FROM base as development

# Stage 3: Production image  
FROM base as production

# Stage 4: CI image
FROM base as ci
```

### Базовый образ (base)

```dockerfile
FROM python:3.11-slim as base

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd --gid 1000 bioactivity && \
    useradd --uid 1000 --gid bioactivity --shell /bin/bash --create-home bioactivity

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY configs/pyproject.toml configs/requirements.txt ./

# Install Python dependencies
RUN pip install --upgrade pip && \
    pip install .[dev]
```

### Образ разработки (development)

```dockerfile
FROM base as development

# Install additional development tools
RUN pip install --no-deps \
    ipython \
    jupyter \
    pre-commit

# Copy source code
COPY src/ ./src/
COPY tests/ ./tests/
COPY configs/ ./configs/
COPY docs/ ./docs/
COPY .pre-commit-config.yaml ./
COPY configs/mkdocs.yml ./

# Copy scripts and configs
COPY scripts/ ./scripts/
COPY .github/ ./.github/

# Set ownership to non-root user
RUN chown -R bioactivity:bioactivity /app

# Switch to non-root user
USER bioactivity

# Default command for development
CMD ["bash"]
```

### Продакшен образ (production)

```dockerfile
FROM base as production

# Copy only necessary files for production
COPY src/ ./src/
COPY configs/ ./configs/

# Install production dependencies only
RUN pip uninstall -y pytest pytest-cov mypy ruff black pre-commit && \
    pip install --no-deps .

# Set ownership to non-root user
RUN chown -R bioactivity:bioactivity /app

# Switch to non-root user
USER bioactivity

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "from library.cli import app; print('Health check passed')" || exit 1

# Default command for production
ENTRYPOINT ["python", "-m", "library.cli"]
```

### CI образ (ci)

```dockerfile
FROM base as ci

# Install CI-specific dependencies including security tools
RUN pip install --no-deps \
    pytest-xdist \
    pytest-benchmark \
    pytest-mock \
    safety \
    bandit

# Copy all source code and tests
COPY . .

# Copy security configuration files
COPY .bandit .bandit
COPY .banditignore .banditignore
COPY .safety_policy.yaml .safety_policy.yaml

# Set ownership to non-root user
RUN chown -R bioactivity:bioactivity /app

# Switch to non-root user
USER bioactivity

# Default command for CI
CMD ["pytest", "--cov=library", "--cov-report=xml", "--cov-report=term-missing"]
```

## Docker Compose

### Основные сервисы

```yaml
version: '3.8'

services:
  # Main bioactivity ETL service
  bioactivity-etl:
    build:
      context: .
      target: development
    container_name: bioactivity-etl-dev
    volumes:
      - .:/app
      - /app/__pycache__
      - /app/.pytest_cache
      - /app/.mypy_cache
    environment:
      - PYTHONPATH=/app/src
      - BIOACTIVITY__HTTP__GLOBAL__TIMEOUT_SEC=30
      - BIOACTIVITY__RUNTIME__WORKERS=2
      - BIOACTIVITY__LOGGING__LEVEL=INFO
    env_file:
      - .env.local  # Optional local environment file
    working_dir: /app
    command: bash
    stdin_open: true
    tty: true
    networks:
      - bioactivity-network
```

### Мониторинг сервисы

```yaml
  # Jaeger for distributed tracing
  jaeger:
    image: jaegertracing/all-in-one:1.50
    container_name: bioactivity-jaeger
    ports:
      - "16686:16686"  # Jaeger UI
      - "14268:14268"  # Jaeger collector HTTP
      - "6831:6831/udp"  # Jaeger agent UDP
    environment:
      - COLLECTOR_OTLP_ENABLED=true
    networks:
      - bioactivity-network

  # Prometheus for metrics (optional)
  prometheus:
    image: prom/prometheus:latest
    container_name: bioactivity-prometheus
    ports:
      - "9090:9090"
    volumes:
      - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml:ro
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.path=/prometheus'
      - '--web.console.libraries=/etc/prometheus/console_libraries'
      - '--web.console.templates=/etc/prometheus/consoles'
      - '--web.enable-lifecycle'
    networks:
      - bioactivity-network

  # Grafana for dashboards (optional)
  grafana:
    image: grafana/grafana:latest
    container_name: bioactivity-grafana
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    volumes:
      - grafana-storage:/var/lib/grafana
      - ./monitoring/grafana/dashboards:/etc/grafana/provisioning/dashboards:ro
      - ./monitoring/grafana/datasources:/etc/grafana/provisioning/datasources:ro
    networks:
      - bioactivity-network
```

### Дополнительные сервисы

```yaml
  # PostgreSQL for data storage (optional)
  postgres:
    image: postgres:15-alpine
    container_name: bioactivity-postgres
    ports:
      - "5432:5432"
    environment:
      - POSTGRES_DB=bioactivity
      - POSTGRES_USER=bioactivity
      - POSTGRES_PASSWORD=password
    volumes:
      - postgres-data:/var/lib/postgresql/data
    networks:
      - bioactivity-network

  # Redis for caching (optional)
  redis:
    image: redis:7-alpine
    container_name: bioactivity-redis
    ports:
      - "6379:6379"
    volumes:
      - redis-data:/data
    networks:
      - bioactivity-network
```

## Запуск пайплайнов в контейнерах

### Сборка образов

```bash
# Сборка всех образов
docker-compose build

# Сборка конкретного образа
docker-compose build bioactivity-etl

# Сборка для продакшена
docker build --target production -t bioactivity-etl:prod .

# Сборка для CI
docker build --target ci -t bioactivity-etl:ci .
```

### Запуск контейнеров

```bash
# Запуск всех сервисов
docker-compose up

# Запуск в фоновом режиме
docker-compose up -d

# Запуск только основного сервиса
docker-compose up bioactivity-etl

# Запуск с пересборкой
docker-compose up --build
```

### Монтирование данных и кэша

```bash
# Запуск с монтированием data/ и кэша
docker-compose run --rm bioactivity-etl \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/.cache:/app/.cache \
  python -m library.cli get-document-data \
  --config configs/config_documents_full.yaml \
  --limit 10
```

### Передача переменных окружения

```bash
# Через файл .env
docker-compose run --rm bioactivity-etl \
  --env-file .env \
  python -m library.cli get-document-data

# Через переменные окружения
docker-compose run --rm bioactivity-etl \
  -e CHEMBL_API_TOKEN=your_token \
  -e PUBMED_API_KEY=your_key \
  python -m library.cli get-document-data
```

## Примеры использования

### Запуск Documents пайплайна

```bash
# Через docker-compose
docker-compose run --rm bioactivity-etl \
  python -m library.cli get-document-data \
  --config configs/config_documents_full.yaml \
  --limit 100

# Через docker run
docker run --rm \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/configs:/app/configs \
  -e CHEMBL_API_TOKEN=your_token \
  bioactivity-etl:prod \
  get-document-data \
  --config configs/config_documents_full.yaml \
  --limit 100
```

### Запуск Targets пайплайна

```bash
# Через docker-compose
docker-compose run --rm bioactivity-etl \
  python -m library.scripts.get_target_data \
  --config configs/config_target_full.yaml \
  --targets-csv data/input/target_ids.csv \
  --output-dir data/output/target

# Через docker run
docker run --rm \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/configs:/app/configs \
  bioactivity-etl:prod \
  python -m library.scripts.get_target_data \
  --config configs/config_target_full.yaml \
  --targets-csv data/input/target_ids.csv
```

### Запуск Assays пайплайна

```bash
# Через docker-compose
docker-compose run --rm bioactivity-etl \
  python src/scripts/get_assay_data.py \
  --target CHEMBL231 \
  --config configs/config_assay_full.yaml \
  --filters human_single_protein

# Через docker run
docker run --rm \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/configs:/app/configs \
  bioactivity-etl:prod \
  python src/scripts/get_assay_data.py \
  --target CHEMBL231 \
  --config configs/config_assay_full.yaml
```

### Запуск Testitems пайплайна

```bash
# Через docker-compose
docker-compose run --rm bioactivity-etl \
  python -m library.cli testitem-run \
  --config configs/config_testitem_full.yaml \
  --input data/input/testitem_keys.csv

# Через docker run
docker run --rm \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/configs:/app/configs \
  bioactivity-etl:prod \
  testitem-run \
  --config configs/config_testitem_full.yaml \
  --input data/input/testitem_keys.csv
```

## Оптимизация

### Слои и кэш сборки

```dockerfile
# Копируем requirements первыми для лучшего кэширования
COPY configs/pyproject.toml configs/requirements.txt ./
RUN pip install .[dev]

# Копируем исходный код после установки зависимостей
COPY src/ ./src/
COPY configs/ ./configs/
```

### Многоэтапная сборка

```bash
# Сборка только для разработки
docker build --target development -t bioactivity-etl:dev .

# Сборка только для продакшена
docker build --target production -t bioactivity-etl:prod .

# Сборка для CI
docker build --target ci -t bioactivity-etl:ci .
```

### Оптимизация размера

```dockerfile
# Используем slim образ
FROM python:3.11-slim

# Очищаем кэш apt
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Удаляем dev зависимости в продакшене
RUN pip uninstall -y pytest pytest-cov mypy ruff black pre-commit
```

## Troubleshooting

### Типовые проблемы с volumes

1. **Volumes не монтируются**
   ```bash
   # Проверьте пути
   docker-compose run --rm bioactivity-etl ls -la /app/data
   
   # Проверьте права доступа
   docker-compose run --rm bioactivity-etl ls -la /app
   ```

2. **Permissions issues**
   ```bash
   # Установите правильные права
   sudo chown -R 1000:1000 data/
   
   # Или используйте пользователя root
   docker-compose run --rm --user root bioactivity-etl chown -R bioactivity:bioactivity /app
   ```

### Проблемы с сетью

1. **Network connectivity**
   ```bash
   # Проверьте сеть
   docker-compose run --rm bioactivity-etl ping google.com
   
   # Проверьте DNS
   docker-compose run --rm bioactivity-etl nslookup google.com
   ```

2. **API недоступны**
   ```bash
   # Проверьте доступность API
   docker-compose run --rm bioactivity-etl \
     python -c "import requests; print(requests.get('https://www.ebi.ac.uk/chembl/api/data/status').status_code)"
   ```

### Проблемы с переменными окружения

1. **Переменные не передаются**
   ```bash
   # Проверьте переменные в контейнере
   docker-compose run --rm bioactivity-etl env | grep CHEMBL
   
   # Проверьте .env файл
   docker-compose run --rm bioactivity-etl cat .env.local
   ```

2. **Неправильный формат переменных**
   ```bash
   # Используйте правильный формат
   export CHEMBL_API_TOKEN=your_token_here
   
   # Или в .env файле
   echo "CHEMBL_API_TOKEN=your_token_here" > .env.local
   ```

### Отладка

```bash
# Запуск с отладкой
docker-compose run --rm bioactivity-etl bash

# Просмотр логов
docker-compose logs bioactivity-etl

# Просмотр логов в реальном времени
docker-compose logs -f bioactivity-etl

# Проверка состояния контейнера
docker-compose ps

# Проверка ресурсов
docker stats bioactivity-etl-dev
```

## Мониторинг

### Jaeger (Distributed Tracing)

```bash
# Запуск Jaeger
docker-compose up jaeger

# Доступ к UI
open http://localhost:16686
```

### Prometheus (Metrics)

```bash
# Запуск Prometheus
docker-compose up prometheus

# Доступ к UI
open http://localhost:9090
```

### Grafana (Dashboards)

```bash
# Запуск Grafana
docker-compose up grafana

# Доступ к UI
open http://localhost:3000
# Логин: admin / admin
```

## Безопасность

### Non-root пользователь

```dockerfile
# Создание non-root пользователя
RUN groupadd --gid 1000 bioactivity && \
    useradd --uid 1000 --gid bioactivity --shell /bin/bash --create-home bioactivity

# Переключение на non-root пользователя
USER bioactivity
```

### Минимальные права

```dockerfile
# Копирование только необходимых файлов
COPY src/ ./src/
COPY configs/ ./configs/

# Удаление dev зависимостей в продакшене
RUN pip uninstall -y pytest pytest-cov mypy ruff black pre-commit
```

### Health checks

```dockerfile
# Health check для продакшена
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "from library.cli import app; print('Health check passed')" || exit 1
```

## CI/CD интеграция

### GitHub Actions

```yaml
# .github/workflows/docker.yml
name: Docker Build

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Build Docker image
        run: docker build --target ci -t bioactivity-etl:ci .
      
      - name: Run tests
        run: docker run --rm bioactivity-etl:ci
```

### Локальная разработка

```bash
# Запуск для разработки
docker-compose up bioactivity-etl

# Выполнение команд в контейнере
docker-compose exec bioactivity-etl bash

# Запуск тестов
docker-compose exec bioactivity-etl pytest tests/

# Форматирование кода
docker-compose exec bioactivity-etl black src/ tests/
```
