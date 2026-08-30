param(
    [datetime]$StartDate,
    [datetime]$EndDate,
    [string[]]$Pages = @("racelist", "odds3t"),
    [int]$SleepMs = 700,
    [int]$TimeoutSec = 30,
    [int]$RetryCount = 2,
    [string]$BaseDir = "",
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

if (-not $StartDate -or -not $EndDate) {
    throw "StartDate and EndDate are required. Example: -StartDate '2024-01-01' -EndDate '2024-12-31'"
}

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectDir = Split-Path -Parent $scriptDir
$collectScript = Join-Path $scriptDir "collect_official_pages.ps1"

if (-not (Test-Path -LiteralPath $collectScript)) {
    throw "collect_official_pages.ps1 not found: $collectScript"
}

if (-not $BaseDir) {
    $BaseDir = Join-Path $projectDir "worker_data"
}

New-Item -ItemType Directory -Force -Path $BaseDir | Out-Null

Write-Host "Worker output: $BaseDir"
Write-Host "Range: $($StartDate.ToString('yyyy-MM-dd')) - $($EndDate.ToString('yyyy-MM-dd'))"
Write-Host "Pages: $($Pages -join ',')"

& $collectScript `
    -BaseDir $BaseDir `
    -StartDate $StartDate `
    -EndDate $EndDate `
    -Pages $Pages `
    -SleepMs $SleepMs `
    -TimeoutSec $TimeoutSec `
    -RetryCount $RetryCount `
    -DryRun:$DryRun
