param(
    [switch]$SkipPytest
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Set-Location (Join-Path $PSScriptRoot "..")

function Invoke-Step {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$Command
    )
    Write-Host ""
    Write-Host "=== $Name ===" -ForegroundColor Cyan
    Write-Host "> $Command"
    Invoke-Expression $Command
    if ($LASTEXITCODE -ne 0) {
        throw "Step failed: $Name (exit code $LASTEXITCODE)"
    }
}

function Assert-RequiredEnv {
    param([Parameter(Mandatory = $true)][string]$Name)
    $value = [Environment]::GetEnvironmentVariable($Name, "Process")
    if ([string]::IsNullOrWhiteSpace($value)) {
        $value = [Environment]::GetEnvironmentVariable($Name, "User")
    }
    if ([string]::IsNullOrWhiteSpace($value)) {
        $value = [Environment]::GetEnvironmentVariable($Name, "Machine")
    }
    if ([string]::IsNullOrWhiteSpace($value)) {
        throw "Missing required environment variable: $Name"
    }
}

# Enforce production preflight expectations.
$env:II_REQUIRE_APP_SECRET_KEY = "1"
$env:II_LOG_JSON = "1"

Assert-RequiredEnv -Name "II_APP_SECRET_KEY"

$hasProviderKey = $false
foreach ($keyName in @("GROQ_API_KEYS", "OPENAI_API_KEYS", "GEMINI_API_KEYS")) {
    $keyValue = [Environment]::GetEnvironmentVariable($keyName, "Process")
    if ([string]::IsNullOrWhiteSpace($keyValue)) {
        $keyValue = [Environment]::GetEnvironmentVariable($keyName, "User")
    }
    if ([string]::IsNullOrWhiteSpace($keyValue)) {
        $keyValue = [Environment]::GetEnvironmentVariable($keyName, "Machine")
    }
    if (-not [string]::IsNullOrWhiteSpace($keyValue)) {
        $hasProviderKey = $true
        break
    }
}
if (-not $hasProviderKey) {
    throw "Missing provider keys: set one of GROQ_API_KEYS / OPENAI_API_KEYS / GEMINI_API_KEYS"
}

try {
    Invoke-Step -Name "Preflight prod" -Command "python scripts/preflight_check.py --prod"
    Invoke-Step -Name "Sanitize runtime artifacts" -Command "python scripts/sanitize_runtime_artifacts.py"
    Invoke-Step -Name "Secret scan" -Command "python scripts/check_secrets.py"
    Invoke-Step -Name "Architecture gate strict" -Command "python scripts/architecture_gate.py --strict"
    Invoke-Step -Name "Release readiness strict" -Command "python scripts/release_readiness_summary.py --strict"
    Invoke-Step -Name "Build timing dashboard (p50/p95)" -Command "python scripts/build_timing_dashboard.py --min-samples 1"
    Invoke-Step -Name "Quality gate strict static" -Command "python scripts/quality_gate.py --strict-static"
    Invoke-Step -Name "Production readiness (preprod policy)" -Command "python scripts/prod_readiness_check.py --env preprod --require-live-data --min-live-ratio 0.25"
    Invoke-Step -Name "Live data gate (strict preprod policy)" -Command "python scripts/live_data_gate_check.py --min-live-ratio 0.25 --require-min-live"
    if (-not $SkipPytest) {
        Invoke-Step -Name "Full pytest" -Command "python -m pytest -q"
    }
    Write-Host ""
    Write-Host "[PASS] Release candidate checks completed." -ForegroundColor Green
    Write-Host "Ready for commercial release workflow (tag/deploy) if business sign-off is done."
    exit 0
}
catch {
    Write-Host ""
    Write-Host "[FAIL] $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}
