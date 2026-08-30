param(
    [string]$BaseDir = "C:\Users\ryuou\Documents\Codex\2026-07-14\c-users-ryuou-documents-codex-2026\bortrace_data",
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

$rawRoot = Join-Path $BaseDir "official_pages"
if (-not (Test-Path -LiteralPath $rawRoot)) {
    throw "official_pages folder not found: $rawRoot"
}

$patterns = @(
    "システムエラー",
    "予期せぬエラーが発生しました",
    "お探しのページは見つかりません",
    "Not Found",
    "Service Unavailable"
)

$results = New-Object System.Collections.Generic.List[object]
$files = Get-ChildItem -LiteralPath $rawRoot -Recurse -File -Filter "*.html"

foreach ($file in $files) {
    $reason = ""

    if ($file.Name.Contains(",")) {
        $reason = "comma_page_name"
    } elseif ($file.Length -le 0) {
        $reason = "empty"
    } else {
        $html = Get-Content -LiteralPath $file.FullName -Raw -Encoding UTF8
        foreach ($pattern in $patterns) {
            if ($html.Contains($pattern)) {
                $reason = "error_page"
                break
            }
        }
    }

    if (-not $reason) {
        continue
    }

    Write-Host "$reason $($file.FullName)"
    $results.Add([pscustomobject]@{
        Path = $file.FullName
        Reason = $reason
        Length = $file.Length
        Deleted = -not $DryRun
    })

    if (-not $DryRun) {
        Remove-Item -LiteralPath $file.FullName -Force
    }
}

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$reportPath = Join-Path $BaseDir "cleanup_invalid_official_pages_$timestamp.csv"
$results | Export-Csv -LiteralPath $reportPath -NoTypeInformation -Encoding UTF8
$results | Group-Object Reason | Sort-Object Name | Select-Object Name,Count | Format-Table -AutoSize
Write-Host "Cleanup report: $reportPath"
