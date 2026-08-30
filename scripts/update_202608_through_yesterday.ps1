param(
    [datetime]$StartDate = [datetime]"2026-08-01",
    [datetime]$EndDate = (Get-Date).Date.AddDays(-1),
    [string]$BaseDir = "C:\Users\ryuou\Documents\Codex\2026-07-14\c-users-ryuou-documents-codex-2026\bortrace_data",
    [string]$DatasetDir = "outputs\official_dataset_parts",
    [string]$PythonExe = "python",
    [int]$SleepMs = 200,
    [int]$TimeoutSec = 30,
    [int]$Workers = 8,
    [ValidateRange(1, 8)]
    [int]$Partitions = 4,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$collector = Join-Path $projectRoot "scripts\collect_official_pages.ps1"
$builder = Join-Path $projectRoot "scripts\build_official_dataset_fast.py"
$datasetRoot = if ([System.IO.Path]::IsPathRooted($DatasetDir)) { $DatasetDir } else { Join-Path $projectRoot $DatasetDir }
$month = $StartDate.ToString("yyyyMM")
$finalPath = Join-Path $datasetRoot "official_race_dataset_$month.csv"
$tempPath = Join-Path $datasetRoot "official_race_dataset_${month}.pending.csv"
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$reportPath = Join-Path $datasetRoot "update_${month}_$timestamp.json"

if ($StartDate -gt $EndDate) {
    throw "StartDate must not be after EndDate."
}
if ($EndDate -ge (Get-Date).Date) {
    throw "EndDate must be yesterday or earlier so unfinished current-day races are not included."
}
if ($StartDate.ToString("yyyyMM") -ne $EndDate.ToString("yyyyMM")) {
    throw "This updater writes one monthly part. StartDate and EndDate must be in the same month."
}
if (-not (Test-Path -LiteralPath $collector) -or -not (Test-Path -LiteralPath $builder)) {
    throw "Required collection/build scripts were not found under $projectRoot\scripts."
}

New-Item -ItemType Directory -Force -Path $datasetRoot | Out-Null
Write-Host "period=$($StartDate.ToString('yyyy-MM-dd'))..$($EndDate.ToString('yyyy-MM-dd'))"
Write-Host "base_dir=$BaseDir"
Write-Host "output=$finalPath"

if ($DryRun) {
    Write-Host "DRY RUN: would collect racelist,beforeinfo,raceresult in $Partitions date partitions and rebuild $month"
    exit 0
}

# Only these three pages are used by official_parser.py. Existing valid pages are skipped.
$totalDays = ($EndDate.Date - $StartDate.Date).Days + 1
$actualPartitions = [Math]::Min($Partitions, $totalDays)
$chunkDays = [Math]::Ceiling($totalDays / $actualPartitions)
$jobs = @()
for ($part = 0; $part -lt $actualPartitions; $part++) {
    $partStart = $StartDate.Date.AddDays($part * $chunkDays)
    if ($partStart -gt $EndDate.Date) { break }
    $partEnd = $partStart.AddDays($chunkDays - 1)
    if ($partEnd -gt $EndDate.Date) { $partEnd = $EndDate.Date }
    Write-Host "start partition=$($part + 1) $($partStart.ToString('yyyy-MM-dd'))..$($partEnd.ToString('yyyy-MM-dd'))"
    $jobs += Start-Job -Name "official_part_$($part + 1)" -ScriptBlock {
        param($CollectorPath, $DataRoot, $FromDate, $ToDate, $DelayMs, $RequestTimeout)
        $ErrorActionPreference = "Stop"
        & $CollectorPath `
            -BaseDir $DataRoot `
            -StartDate $FromDate `
            -EndDate $ToDate `
            -Pages @("racelist", "beforeinfo", "raceresult") `
            -SleepMs $DelayMs `
            -TimeoutSec $RequestTimeout `
            -RetryCount 2 `
            -ProgressEvery 100
        if (-not $?) { throw "collector failed for $FromDate..$ToDate" }
    } -ArgumentList $collector, $BaseDir, $partStart, $partEnd, $SleepMs, $TimeoutSec
}

try {
    $jobs | Wait-Job | Receive-Job
    $failedJobs = @($jobs | Where-Object State -ne "Completed")
    if ($failedJobs.Count -gt 0) {
        throw "One or more collection partitions failed: $($failedJobs.Name -join ', ')"
    }
} finally {
    $jobs | Remove-Job -Force -ErrorAction SilentlyContinue
}

if (Test-Path -LiteralPath $tempPath) {
    Remove-Item -LiteralPath $tempPath -Force
}
$startText = $StartDate.ToString("yyyyMMdd")
$endText = $EndDate.ToString("yyyyMMdd")
& $PythonExe $builder `
    --start-date $startText `
    --end-date $endText `
    --out $tempPath `
    --workers $Workers `
    --progress-every 500
if ($LASTEXITCODE -ne 0 -or -not (Test-Path -LiteralPath $tempPath)) {
    throw "Monthly dataset build failed with exit code $LASTEXITCODE"
}

$rows = @(Import-Csv -LiteralPath $tempPath)
if ($rows.Count -eq 0) {
    throw "Built dataset is empty; refusing to replace $finalPath"
}
$required = @("date", "jcd", "rno", "target_pos1", "target_pos2", "target_pos3", "target_pos4", "target_pos5", "target_pos6")
$headers = @($rows[0].PSObject.Properties.Name)
$missingHeaders = @($required | Where-Object { $_ -notin $headers })
if ($missingHeaders.Count -gt 0) {
    throw "Built dataset is missing required columns: $($missingHeaders -join ', ')"
}
$badTargets = @($rows | Where-Object {
    -not $_.target_pos1 -or -not $_.target_pos2 -or -not $_.target_pos3 -or
    -not $_.target_pos4 -or -not $_.target_pos5 -or -not $_.target_pos6
})
$duplicates = @($rows | Group-Object date,jcd,rno | Where-Object Count -gt 1)
$minDate = ($rows | Measure-Object -Property date -Minimum).Minimum
$maxDate = ($rows | Measure-Object -Property date -Maximum).Maximum
if ($duplicates.Count -gt 0) {
    throw "Duplicate race keys found: $($duplicates.Count); refusing to replace monthly dataset."
}
if ($badTargets.Count -gt 0) {
    throw "Rows with incomplete order targets found: $($badTargets.Count); refusing to replace monthly dataset."
}
if ([string]$maxDate -ne $EndDate.ToString("yyyyMMdd")) {
    Write-Warning "Latest parsed race date is $maxDate, expected $($EndDate.ToString('yyyyMMdd')). Check whether the end date had開催."
}

$backupPath = $null
if (Test-Path -LiteralPath $finalPath) {
    $backupPath = "$finalPath.backup_$timestamp"
    Copy-Item -LiteralPath $finalPath -Destination $backupPath
}
Move-Item -LiteralPath $tempPath -Destination $finalPath -Force

$summary = [ordered]@{
    start_date = $StartDate.ToString("yyyyMMdd")
    end_date = $EndDate.ToString("yyyyMMdd")
    rows = $rows.Count
    min_date = [string]$minDate
    max_date = [string]$maxDate
    duplicate_races = $duplicates.Count
    incomplete_targets = $badTargets.Count
    output = $finalPath
    backup = $backupPath
}
$summary | ConvertTo-Json -Depth 3 | Set-Content -LiteralPath $reportPath -Encoding UTF8
Write-Host ($summary | ConvertTo-Json -Depth 3)
Write-Host "report=$reportPath"
