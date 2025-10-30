# Скрипт для запуска тестов на Windows
Write-Host "Running tests..." -ForegroundColor Cyan

python -m pytest tests/ -v

if ($LASTEXITCODE -eq 0) {
    Write-Host "Tests completed!" -ForegroundColor Green
} else {
    Write-Host "Tests failed!" -ForegroundColor Red
    exit $LASTEXITCODE
}

