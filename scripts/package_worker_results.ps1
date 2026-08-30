param(
    [string]$BaseDir = "",
    [string]$OutDir = ""
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectDir = Split-Path -Parent $scriptDir

if (-not $BaseDir) {
    $BaseDir = Join-Path $projectDir "worker_data"
}

if (-not $OutDir) {
    $OutDir = Join-Path $projectDir "worker_packages"
}

if (-not (Test-Path -LiteralPath $BaseDir)) {
    throw "BaseDir not found: $BaseDir"
}

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$zipPath = Join-Path $OutDir "official_pages_worker_$stamp.zip"

Compress-Archive -LiteralPath (Join-Path $BaseDir "official_pages") -DestinationPath $zipPath -Force

$reports = Get-ChildItem -LiteralPath $BaseDir -File -Filter "*.csv" -ErrorAction SilentlyContinue
if ($reports.Count -gt 0) {
    $reportZip = Join-Path $OutDir "official_pages_worker_reports_$stamp.zip"
    Compress-Archive -LiteralPath $reports.FullName -DestinationPath $reportZip -Force
    Write-Host "Report zip: $reportZip"
}

Write-Host "Data zip: $zipPath"
