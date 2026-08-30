param(
    [string]$BaseDir = "C:\Users\ryuou\Documents\Codex\2026-07-14\c-users-ryuou-documents-codex-2026\bortrace_data",
    [string[]]$ReportPaths = @(),
    [string[]]$Statuses = @("error", "empty", "extract_missing", "invalid_html"),
    [string[]]$Pages = @("racelist", "beforeinfo", "odds3t", "raceresult"),
    [int]$SleepMs = 250,
    [int]$TimeoutSec = 45,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

function Get-DefaultReports {
    param([string]$Root)

    $reports = @()
    $bk = Get-ChildItem -LiteralPath $Root -Filter "download_*_report.csv" -File -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1
    $official = Get-ChildItem -LiteralPath $Root -Filter "official_pages_*_report.csv" -File -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1

    if ($bk) { $reports += $bk.FullName }
    if ($official) { $reports += $official.FullName }
    return $reports
}

function Add-Result {
    param(
        [System.Collections.Generic.List[object]]$Results,
        [object]$Row,
        [string]$RetryStatus,
        [string]$Message
    )

    $Results.Add([pscustomobject]@{
        Date = $Row.Date
        Type = $Row.Type
        Jcd = $Row.Jcd
        Rno = $Row.Rno
        Kind = $Row.Kind
        OldStatus = $Row.Status
        RetryStatus = $RetryStatus
        Path = $Row.Path
        Url = $Row.Url
        Message = $Message
    })
}

function Retry-BkRow {
    param(
        [object]$Row,
        [System.Collections.Generic.List[object]]$Results
    )

    $txtPath = $Row.Path
    $url = $Row.Url
    if (-not $txtPath -or -not $url) {
        Add-Result -Results $Results -Row $Row -RetryStatus "skipped" -Message "missing path or url"
        return
    }

    if ((Test-Path -LiteralPath $txtPath) -and ((Get-Item -LiteralPath $txtPath).Length -gt 0)) {
        Write-Host "$($Row.Date) $($Row.Type) exists"
        Add-Result -Results $Results -Row $Row -RetryStatus "exists" -Message ""
        return
    }

    if ($DryRun) {
        Write-Host "$($Row.Date) $($Row.Type) dry-run $url"
        Add-Result -Results $Results -Row $Row -RetryStatus "dry-run" -Message ""
        return
    }

    try {
        $lzhDir = Join-Path $BaseDir "lzh"
        $txtDir = Join-Path $BaseDir "txt"
        New-Item -ItemType Directory -Force -Path $lzhDir | Out-Null
        New-Item -ItemType Directory -Force -Path $txtDir | Out-Null

        $lzhName = Split-Path -Leaf $url
        $lzhPath = Join-Path $lzhDir $lzhName

        Write-Host "$($Row.Date) $($Row.Type) retry"
        Invoke-WebRequest -Uri $url -OutFile $lzhPath -TimeoutSec $TimeoutSec
        tar -xf $lzhPath -C $txtDir

        if ((Test-Path -LiteralPath $txtPath) -and ((Get-Item -LiteralPath $txtPath).Length -gt 0)) {
            Add-Result -Results $Results -Row $Row -RetryStatus "success" -Message ""
        } else {
            Add-Result -Results $Results -Row $Row -RetryStatus "extract_missing" -Message ""
        }
    } catch {
        Add-Result -Results $Results -Row $Row -RetryStatus "error" -Message $_.Exception.Message
    }

    Start-Sleep -Milliseconds $SleepMs
}

function Retry-OfficialRow {
    param(
        [object]$Row,
        [System.Collections.Generic.List[object]]$Results
    )

    $path = $Row.Path
    $url = $Row.Url
    if (-not $path -or -not $url) {
        Add-Result -Results $Results -Row $Row -RetryStatus "skipped" -Message "missing path or url"
        return
    }

    if ((Test-Path -LiteralPath $path) -and ((Get-Item -LiteralPath $path).Length -gt 0)) {
        Write-Host "$($Row.Date) jcd=$($Row.Jcd) rno=$($Row.Rno) $($Row.Kind) exists"
        Add-Result -Results $Results -Row $Row -RetryStatus "exists" -Message ""
        return
    }

    if ($DryRun) {
        Write-Host "$($Row.Date) jcd=$($Row.Jcd) rno=$($Row.Rno) $($Row.Kind) dry-run $url"
        Add-Result -Results $Results -Row $Row -RetryStatus "dry-run" -Message ""
        return
    }

    try {
        New-Item -ItemType Directory -Force -Path (Split-Path -Parent $path) | Out-Null
        Write-Host "$($Row.Date) jcd=$($Row.Jcd) rno=$($Row.Rno) $($Row.Kind) retry"
        Invoke-WebRequest -Uri $url -OutFile $path -TimeoutSec $TimeoutSec

        if ((Test-Path -LiteralPath $path) -and ((Get-Item -LiteralPath $path).Length -gt 0)) {
            Add-Result -Results $Results -Row $Row -RetryStatus "success" -Message ""
        } else {
            Add-Result -Results $Results -Row $Row -RetryStatus "empty" -Message ""
        }
    } catch {
        Add-Result -Results $Results -Row $Row -RetryStatus "error" -Message $_.Exception.Message
    }

    Start-Sleep -Milliseconds $SleepMs
}

function Retry-NoVenueDays {
    param(
        [object[]]$Rows,
        [System.Collections.Generic.List[object]]$Results
    )

    $dates = $Rows | Where-Object { $_.Status -eq "no_venues" -and $_.Date } |
        Select-Object -ExpandProperty Date -Unique |
        Sort-Object

    foreach ($date in $dates) {
        $start = [datetime]::ParseExact($date, "yyyyMMdd", $null)
        Write-Host "$date no_venues retry whole day"

        if ($DryRun) {
            $fake = [pscustomobject]@{ Date = $date; Type = ""; Jcd = ""; Rno = ""; Kind = "day"; Status = "no_venues"; Path = ""; Url = "" }
            Add-Result -Results $Results -Row $fake -RetryStatus "dry-run" -Message "would run collect_official_pages.ps1 for this day"
            continue
        }

        try {
            & (Join-Path $scriptDir "collect_official_pages.ps1") `
                -BaseDir $BaseDir `
                -StartDate $start `
                -EndDate $start `
                -Pages $Pages `
                -SleepMs $SleepMs `
                -TimeoutSec $TimeoutSec

            $fake = [pscustomobject]@{ Date = $date; Type = ""; Jcd = ""; Rno = ""; Kind = "day"; Status = "no_venues"; Path = ""; Url = "" }
            Add-Result -Results $Results -Row $fake -RetryStatus "reran_day" -Message ""
        } catch {
            $fake = [pscustomobject]@{ Date = $date; Type = ""; Jcd = ""; Rno = ""; Kind = "day"; Status = "no_venues"; Path = ""; Url = "" }
            Add-Result -Results $Results -Row $fake -RetryStatus "error" -Message $_.Exception.Message
        }
    }
}

if ($ReportPaths.Count -eq 0) {
    $ReportPaths = Get-DefaultReports -Root $BaseDir
}

if ($ReportPaths.Count -eq 0) {
    throw "No report CSV found. Pass -ReportPaths or check BaseDir."
}

$retryResults = New-Object System.Collections.Generic.List[object]

foreach ($reportPath in $ReportPaths) {
    if (-not (Test-Path -LiteralPath $reportPath)) {
        Write-Host "missing report: $reportPath"
        continue
    }

    Write-Host "Report: $reportPath"
    $rows = Import-Csv -LiteralPath $reportPath
    $failed = $rows | Where-Object { $Statuses -contains $_.Status }

    foreach ($row in $failed) {
        if ($row.Type -in @("B", "K")) {
            Retry-BkRow -Row $row -Results $retryResults
        } elseif ($row.Status -ne "no_venues") {
            Retry-OfficialRow -Row $row -Results $retryResults
        }
    }

    if ($Statuses -contains "no_venues") {
        Retry-NoVenueDays -Rows $rows -Results $retryResults
    }
}

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$retryReportPath = Join-Path $BaseDir "retry_failed_downloads_$timestamp.csv"
$retryResults | Export-Csv -LiteralPath $retryReportPath -NoTypeInformation -Encoding UTF8
$retryResults | Group-Object RetryStatus | Sort-Object Name | Select-Object Name,Count | Format-Table -AutoSize
Write-Host "Retry report: $retryReportPath"
