# PowerShell test for Makefile functionality
# Tests basic Makefile commands and PowerShell wrapper functionality

param(
    [string]$MakefilePath = "Makefile",
    [string]$TestCommand = "help"
)

Write-Host "Testing Makefile PowerShell integration..." -ForegroundColor Blue

# Test if Makefile exists
if (-not (Test-Path $MakefilePath)) {
    Write-Error "Makefile not found at: $MakefilePath"
    exit 1
}

Write-Host "✓ Makefile found at: $MakefilePath" -ForegroundColor Green

# Test make help command
Write-Host "Testing 'make help' command..." -ForegroundColor Yellow
try {
    $helpOutput = & make help 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✓ 'make help' executed successfully" -ForegroundColor Green
        Write-Host "Help output preview:" -ForegroundColor Cyan
        $helpOutput | Select-Object -First 5 | ForEach-Object { Write-Host "  $_" }
    } else {
        Write-Error "make help failed with exit code: $LASTEXITCODE"
        exit 1
    }
} catch {
    Write-Error "Failed to execute make help: $_"
    exit 1
}

# Test make version command (if available)
Write-Host "Testing 'make version' command..." -ForegroundColor Yellow
try {
    $versionOutput = & make version 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✓ 'make version' executed successfully" -ForegroundColor Green
    } else {
        Write-Host "⚠ 'make version' not available (exit code: $LASTEXITCODE)" -ForegroundColor Yellow
    }
} catch {
    Write-Host "⚠ 'make version' not available: $_" -ForegroundColor Yellow
}

Write-Host "PowerShell Makefile tests completed successfully!" -ForegroundColor Green
