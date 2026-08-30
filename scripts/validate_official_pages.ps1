param(
    [string]$BaseDir = "C:\Users\ryuou\Documents\Codex\2026-07-14\c-users-ryuou-documents-codex-2026\bortrace_data",
    [string[]]$Pages = @("racelist", "beforeinfo", "odds3t", "raceresult")
)

$ErrorActionPreference = "Stop"

$rawRoot = Join-Path $BaseDir "official_pages"
if (-not (Test-Path -LiteralPath $rawRoot)) {
    throw "official_pages folder not found: $rawRoot"
}

function Test-ErrorHtml {
    param([string]$Path)

    if (-not (Test-Path -LiteralPath $Path)) {
        return $false
    }

    $bytes = [System.IO.File]::ReadAllBytes($Path)
    $hex = [System.BitConverter]::ToString($bytes).Replace("-", "").ToLowerInvariant()
    $ascii = [System.Text.Encoding]::ASCII.GetString($bytes)
    $utf8ErrorHexPatterns = @(
        "e382b7e382b9e38386e383a0e382a8e383a9e383bc",
        "e4ba88e69c9fe3819be381ace382a8e383a9e383bce3818ce799bae7949fe38197e381bee38197e3819f",
        "e3818ae68ea2e38197e381aee3839ae383bce382b8e381afe8a68be381a4e3818be3828ae381bee3819be38293"
    )

    foreach ($pattern in $utf8ErrorHexPatterns) {
        if ($hex.Contains($pattern)) {
            return $true
        }
    }

    foreach ($pattern in @("Not Found", "Service Unavailable")) {
        if ($ascii.Contains($pattern)) {
            return $true
        }
    }

    return $false
}

function Test-ContentSignals {
    param(
        [string]$Path,
        [string]$Page
    )

    if (-not (Test-Path -LiteralPath $Path)) {
        return $false
    }

    return ((Get-Item -LiteralPath $Path).Length -gt 1000)
}

$files = Get-ChildItem -LiteralPath $rawRoot -Recurse -File -Filter "*.html"
$bad = New-Object System.Collections.Generic.List[object]
$missing = New-Object System.Collections.Generic.List[object]
$present = New-Object System.Collections.Generic.List[object]

foreach ($file in $files) {
    if ($file.Name.Contains(",")) {
        $bad.Add([pscustomobject]@{ Path = $file.FullName; Reason = "comma_page_name" })
        continue
    }
    if ($file.Length -le 0) {
        $bad.Add([pscustomobject]@{ Path = $file.FullName; Reason = "empty" })
        continue
    }
    if (Test-ErrorHtml -Path $file.FullName) {
        $bad.Add([pscustomobject]@{ Path = $file.FullName; Reason = "error_page" })
    }
}

$raceIndexes = Get-ChildItem -LiteralPath $rawRoot -Recurse -File -Filter "raceindex.html"
foreach ($raceIndex in $raceIndexes) {
    $date = Split-Path -Leaf (Split-Path -Parent (Split-Path -Parent $raceIndex.FullName))
    $jcd = Split-Path -Leaf (Split-Path -Parent $raceIndex.FullName)
    $html = Get-Content -LiteralPath $raceIndex.FullName -Raw -Encoding UTF8
    $races = [regex]::Matches($html, "rno=(\d{1,2})") |
        ForEach-Object { [int]$_.Groups[1].Value } |
        Where-Object { $_ -ge 1 -and $_ -le 12 } |
        Sort-Object -Unique

    foreach ($rno in $races) {
        foreach ($page in $Pages) {
            $path = Join-Path $rawRoot "$date\$jcd\$rno\$page.html"
            if (-not (Test-Path -LiteralPath $path)) {
                $missing.Add([pscustomobject]@{ Date = $date; Jcd = $jcd; Rno = $rno; Page = $page; Status = "missing"; Path = $path })
                continue
            }

            if (Test-ErrorHtml -Path $path) {
                $missing.Add([pscustomobject]@{ Date = $date; Jcd = $jcd; Rno = $rno; Page = $page; Status = "error_page"; Path = $path })
                continue
            }

            if (-not (Test-ContentSignals -Path $path -Page $page)) {
                $missing.Add([pscustomobject]@{ Date = $date; Jcd = $jcd; Rno = $rno; Page = $page; Status = "weak_content_signal"; Path = $path })
                continue
            }

            $present.Add([pscustomobject]@{ Date = $date; Jcd = $jcd; Rno = $rno; Page = $page; Status = "ok"; Path = $path })
        }
    }
}

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$missingReport = Join-Path $BaseDir "validate_official_missing_$timestamp.csv"
$badReport = Join-Path $BaseDir "validate_official_bad_html_$timestamp.csv"
$presentReport = Join-Path $BaseDir "validate_official_present_$timestamp.csv"

$missing | Export-Csv -LiteralPath $missingReport -NoTypeInformation -Encoding UTF8
$bad | Export-Csv -LiteralPath $badReport -NoTypeInformation -Encoding UTF8
$present | Export-Csv -LiteralPath $presentReport -NoTypeInformation -Encoding UTF8

Write-Host "HTML files: $($files.Count)"
Write-Host "Race index files: $($raceIndexes.Count)"
Write-Host "Expected race pages from raceindex: $($missing.Count + $present.Count)"
Write-Host "Present wanted pages: $($present.Count)"
Write-Host "Missing/invalid wanted pages: $($missing.Count)"
Write-Host "Bad HTML files: $($bad.Count)"
Write-Host "Missing report: $missingReport"
Write-Host "Bad HTML report: $badReport"
Write-Host "Present report: $presentReport"

if ($missing.Count -gt 0) {
    $missing | Group-Object Page,Status | Sort-Object Name | Select-Object Name,Count | Format-Table -AutoSize
}
