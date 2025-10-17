# Скрипт для установки API ключей в переменные окружения
# Setup script for API keys environment variables

param(
    [string]$SemanticScholarKey = "o2N1y1RHYU3aqEj556Oyv4oBzZrHthM2bWda2lf4",
    [string]$ChemblToken = "",
    [string]$CrossrefKey = "",
    [string]$OpenAlexKey = "",
    [string]$PubmedKey = "",
    [switch]$Persistent = $false,
    [switch]$ShowHelp = $false
)

if ($ShowHelp) {
    Write-Host @"
Скрипт для установки API ключей в переменные окружения

Использование:
    .\scripts\setup_api_keys.ps1 [параметры]

Параметры:
    -SemanticScholarKey <key>  : API ключ для Semantic Scholar (по умолчанию: предустановленный)
    -ChemblToken <token>       : API токен для ChEMBL
    -CrossrefKey <key>         : API ключ для Crossref
    -OpenAlexKey <key>         : API ключ для OpenAlex
    -PubmedKey <key>           : API ключ для PubMed
    -Persistent                : Установить переменные постоянно (требует перезапуска PowerShell)
    -ShowHelp                  : Показать эту справку

Примеры:
    # Установить только Semantic Scholar ключ (по умолчанию)
    .\scripts\setup_api_keys.ps1

    # Установить все ключи
    .\scripts\setup_api_keys.ps1 -ChemblToken "your_token" -CrossrefKey "your_key"

    # Установить постоянно
    .\scripts\setup_api_keys.ps1 -Persistent

    # Установить кастомный Semantic Scholar ключ
    .\scripts\setup_api_keys.ps1 -SemanticScholarKey "your_custom_key"
"@
    exit 0
}

Write-Host "🔑 Настройка API ключей..." -ForegroundColor Green

# Функция для установки переменной окружения
function Set-EnvironmentVariable {
    param(
        [string]$Name,
        [string]$Value,
        [bool]$Persistent = $false
    )
    
    if ([string]::IsNullOrEmpty($Value)) {
        Write-Host "⚠️  Пропуск $Name (значение не указано)" -ForegroundColor Yellow
        return
    }
    
    # Установка для текущей сессии
    Set-Item -Path "env:$Name" -Value $Value
    Write-Host "✅ $Name установлен для текущей сессии" -ForegroundColor Green
    
    # Установка постоянно (если запрошено)
    if ($Persistent) {
        try {
            [Environment]::SetEnvironmentVariable($Name, $Value, "User")
            Write-Host "✅ $Name установлен постоянно (требует перезапуска PowerShell)" -ForegroundColor Green
        }
        catch {
            Write-Host "❌ Ошибка при установке $Name постоянно: $($_.Exception.Message)" -ForegroundColor Red
        }
    }
}

# Установка API ключей
Set-EnvironmentVariable -Name "SEMANTIC_SCHOLAR_API_KEY" -Value $SemanticScholarKey -Persistent $Persistent
Set-EnvironmentVariable -Name "CHEMBL_API_TOKEN" -Value $ChemblToken -Persistent $Persistent
Set-EnvironmentVariable -Name "CROSSREF_API_KEY" -Value $CrossrefKey -Persistent $Persistent
Set-EnvironmentVariable -Name "OPENALEX_API_KEY" -Value $OpenAlexKey -Persistent $Persistent
Set-EnvironmentVariable -Name "PUBMED_API_KEY" -Value $PubmedKey -Persistent $Persistent

Write-Host ""
Write-Host "🎯 Проверка установленных переменных:" -ForegroundColor Cyan

# Проверка установленных переменных
$envVars = @(
    "SEMANTIC_SCHOLAR_API_KEY",
    "CHEMBL_API_TOKEN", 
    "CROSSREF_API_KEY",
    "OPENALEX_API_KEY",
    "PUBMED_API_KEY"
)

foreach ($var in $envVars) {
    $value = [Environment]::GetEnvironmentVariable($var, "Process")
    if ($value) {
        $displayValue = if ($value.Length -gt 10) { $value.Substring(0, 10) + "..." } else { $value }
        Write-Host "  ✅ $var = $displayValue" -ForegroundColor Green
    } else {
        Write-Host "  ❌ $var = не установлен" -ForegroundColor Red
    }
}

Write-Host ""
Write-Host "🚀 Готово! Теперь можно запускать команды:" -ForegroundColor Green
Write-Host "   bioactivity-data-acquisition get-document-data --config configs\config_documents_full.yaml --limit 10" -ForegroundColor White

if ($Persistent) {
    Write-Host ""
    Write-Host "⚠️  Для применения постоянных переменных перезапустите PowerShell" -ForegroundColor Yellow
}
