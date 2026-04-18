# Chay Streamlit trong PowerShell: chuot phai -> Run with PowerShell, hoac: .\run_app.ps1
Set-Location $PSScriptRoot

# Fast startup profile
$env:II_SNAPSHOT_ATTACH_LIVE = "0"
$env:II_READ_STALE_DISK = "1"
$env:VALUE_INVESTOR_PORTAL_TIMEOUT = "4"
$env:II_PORTAL_TIMEOUT_LADDER_SEC = "2,4"
$env:II_ALIGN_PRICE_WITH_OHLCV = "1"
$env:II_OHLCV_DISK_FIRST = "1"
$env:II_OHLCV_DISK_MAX_AGE_SEC = "7200"
$env:II_FINANCIAL_DISK_FIRST = "1"
$env:II_FINANCIAL_DISK_MAX_AGE_SEC = "21600"
$env:II_FINANCIAL_MAX_PROBES = "1"
$env:II_PORTAL_LIVE_BUDGET_SEC = "6"
$env:II_LEGEND_PROFILE = "defensive"
$env:II_LEGEND_STRONG_BUY_MOS_MIN = "8"
$env:II_LEGEND_MAX_PEG_FOR_BUY = "1.1"
$env:II_LEGEND_WATCH_BUY_MOS_MIN = "8"

$py = Join-Path $env:LOCALAPPDATA "Programs\Python\Python312\python.exe"
if (Test-Path -LiteralPath $py) {
    & $py -m streamlit run app.py
    exit $LASTEXITCODE
}
if (Get-Command py -ErrorAction SilentlyContinue) {
    py -m streamlit run app.py
    exit $LASTEXITCODE
}
if (Get-Command python -ErrorAction SilentlyContinue) {
    python -m streamlit run app.py
    exit $LASTEXITCODE
}
Write-Host "Khong tim thay Python. Cai Python 3 hoac them vao PATH."
exit 1
