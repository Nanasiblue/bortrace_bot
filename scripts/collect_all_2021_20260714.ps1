param(
    [string]$BaseDir = "C:\Users\ryuou\Documents\Codex\2026-07-14\c-users-ryuou-documents-codex-2026\bortrace_data",
    [switch]$SkipBk,
    [switch]$SkipOfficialPages,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

if (-not $SkipBk) {
    & (Join-Path $scriptDir "download_range.ps1") `
        -BaseDir $BaseDir `
        -StartDate "2021-01-01" `
        -EndDate "2026-07-14" `
        -DryRun:$DryRun
}

if (-not $SkipOfficialPages) {
    & (Join-Path $scriptDir "collect_official_pages.ps1") `
        -BaseDir $BaseDir `
        -StartDate "2021-01-01" `
        -EndDate "2026-07-14" `
        -DryRun:$DryRun
}
