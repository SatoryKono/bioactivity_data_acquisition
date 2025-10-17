#!/bin/bash
# Скрипт для установки API ключей в переменные окружения
# Setup script for API keys environment variables

# Значения по умолчанию
SEMANTIC_SCHOLAR_KEY="o2N1y1RHYU3aqEj556Oyv4oBzZrHthM2bWda2lf4"
CHEMBL_TOKEN=""
CROSSREF_KEY=""
OPENALEX_KEY=""
PUBMED_KEY=""
PERSISTENT=false

# Функция для показа справки
show_help() {
    cat << EOF
Скрипт для установки API ключей в переменные окружения

Использование:
    ./scripts/setup_api_keys.sh [параметры]

Параметры:
    -s, --semantic-scholar <key>  : API ключ для Semantic Scholar (по умолчанию: предустановленный)
    -c, --chembl <token>          : API токен для ChEMBL
    -r, --crossref <key>          : API ключ для Crossref
    -o, --openalex <key>          : API ключ для OpenAlex
    -p, --pubmed <key>            : API ключ для PubMed
    --persistent                  : Установить переменные постоянно (добавить в ~/.bashrc)
    -h, --help                    : Показать эту справку

Примеры:
    # Установить только Semantic Scholar ключ (по умолчанию)
    ./scripts/setup_api_keys.sh

    # Установить все ключи
    ./scripts/setup_api_keys.sh -c "your_token" -r "your_key"

    # Установить постоянно
    ./scripts/setup_api_keys.sh --persistent

    # Установить кастомный Semantic Scholar ключ
    ./scripts/setup_api_keys.sh -s "your_custom_key"
EOF
}

# Парсинг аргументов
while [[ $# -gt 0 ]]; do
    case $1 in
        -s|--semantic-scholar)
            SEMANTIC_SCHOLAR_KEY="$2"
            shift 2
            ;;
        -c|--chembl)
            CHEMBL_TOKEN="$2"
            shift 2
            ;;
        -r|--crossref)
            CROSSREF_KEY="$2"
            shift 2
            ;;
        -o|--openalex)
            OPENALEX_KEY="$2"
            shift 2
            ;;
        -p|--pubmed)
            PUBMED_KEY="$2"
            shift 2
            ;;
        --persistent)
            PERSISTENT=true
            shift
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            echo "Неизвестный параметр: $1"
            show_help
            exit 1
            ;;
    esac
done

echo "🔑 Настройка API ключей..."

# Функция для установки переменной окружения
set_env_var() {
    local name="$1"
    local value="$2"
    local persistent="$3"
    
    if [ -z "$value" ]; then
        echo "⚠️  Пропуск $name (значение не указано)"
        return
    fi
    
    # Установка для текущей сессии
    export "$name"="$value"
    echo "✅ $name установлен для текущей сессии"
    
    # Установка постоянно (если запрошено)
    if [ "$persistent" = true ]; then
        # Определяем файл профиля
        local profile_file=""
        if [ -n "$ZSH_VERSION" ]; then
            profile_file="$HOME/.zshrc"
        elif [ -n "$BASH_VERSION" ]; then
            profile_file="$HOME/.bashrc"
        else
            profile_file="$HOME/.profile"
        fi
        
        # Проверяем, не установлена ли уже переменная
        if ! grep -q "export $name=" "$profile_file" 2>/dev/null; then
            echo "export $name=\"$value\"" >> "$profile_file"
            echo "✅ $name добавлен в $profile_file"
        else
            echo "⚠️  $name уже установлен в $profile_file"
        fi
    fi
}

# Установка API ключей
set_env_var "SEMANTIC_SCHOLAR_API_KEY" "$SEMANTIC_SCHOLAR_KEY" "$PERSISTENT"
set_env_var "CHEMBL_API_TOKEN" "$CHEMBL_TOKEN" "$PERSISTENT"
set_env_var "CROSSREF_API_KEY" "$CROSSREF_KEY" "$PERSISTENT"
set_env_var "OPENALEX_API_KEY" "$OPENALEX_KEY" "$PERSISTENT"
set_env_var "PUBMED_API_KEY" "$PUBMED_KEY" "$PERSISTENT"

echo ""
echo "🎯 Проверка установленных переменных:"

# Проверка установленных переменных
env_vars=(
    "SEMANTIC_SCHOLAR_API_KEY"
    "CHEMBL_API_TOKEN"
    "CROSSREF_API_KEY"
    "OPENALEX_API_KEY"
    "PUBMED_API_KEY"
)

for var in "${env_vars[@]}"; do
    value=$(eval echo \$$var)
    if [ -n "$value" ]; then
        display_value=$(echo "$value" | cut -c1-10)
        if [ ${#value} -gt 10 ]; then
            display_value="${display_value}..."
        fi
        echo "  ✅ $var = $display_value"
    else
        echo "  ❌ $var = не установлен"
    fi
done

echo ""
echo "🚀 Готово! Теперь можно запускать команды:"
echo "   bioactivity-data-acquisition get-document-data --config configs/config_documents_full.yaml --limit 10"

if [ "$PERSISTENT" = true ]; then
    echo ""
    echo "⚠️  Для применения постоянных переменных выполните: source ~/.bashrc (или перезапустите терминал)"
fi
