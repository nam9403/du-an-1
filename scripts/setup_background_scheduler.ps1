param(
    [string]$TaskName = "DuAn1_BackgroundJobs",
    [int]$EveryMinutes = 15,
    [string]$Watchlist = "FPT,HPG,VNM",
    [int]$UniverseLimit = 30,
    [int]$QueueJobs = 30
)

$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$python = Join-Path $env:LOCALAPPDATA "Programs\Python\Python312\python.exe"
if (-not (Test-Path $python)) { $python = "python" }

$scriptPath = Join-Path $projectRoot "scripts\run_background_jobs.py"
$args = "`"$scriptPath`" --watchlist $Watchlist --universe-limit $UniverseLimit --queue-jobs $QueueJobs"

Write-Host "Creating/updating task: $TaskName"
schtasks /Create /F /SC MINUTE /MO $EveryMinutes /TN $TaskName /TR "`"$python`" $args" | Out-Null

Write-Host "Task created:"
schtasks /Query /TN $TaskName /V /FO LIST
Write-Host ""
Write-Host "Done. Scheduler will run every $EveryMinutes minute(s)."
