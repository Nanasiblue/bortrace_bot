param(
    [Parameter(Mandatory = $true)]
    [string]$ZipPath,

    [string]$DataDir = ""
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectDir = Split-Path -Parent $scriptDir

if (-not $DataDir) {
    $DataDir = Join-Path (Split-Path -Parent $projectDir) "bortrace_data"
}

$zipFullPath = [System.IO.Path]::GetFullPath($ZipPath)
if (-not (Test-Path -LiteralPath $zipFullPath)) {
    throw "Zip not found: $zipFullPath"
}

$officialDest = Join-Path $DataDir "official_pages"
New-Item -ItemType Directory -Force -Path $officialDest | Out-Null

$tempRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("bortrace_worker_import_" + [guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Force -Path $tempRoot | Out-Null

try {
    Expand-Archive -LiteralPath $zipFullPath -DestinationPath $tempRoot -Force

    $officialSrc = Join-Path $tempRoot "official_pages"
    if (-not (Test-Path -LiteralPath $officialSrc)) {
        $candidate = Get-ChildItem -LiteralPath $tempRoot -Directory -Recurse |
            Where-Object { $_.Name -eq "official_pages" } |
            Select-Object -First 1
        if ($candidate) {
            $officialSrc = $candidate.FullName
        }
    }

    if (-not (Test-Path -LiteralPath $officialSrc)) {
        throw "official_pages folder not found in zip: $zipFullPath"
    }

    $files = Get-ChildItem -LiteralPath $officialSrc -File -Recurse
    $copied = 0
    $skipped = 0

    foreach ($file in $files) {
        $relative = $file.FullName.Substring($officialSrc.Length).TrimStart('\', '/')
        $dest = Join-Path $officialDest $relative
        $destDir = Split-Path -Parent $dest
        New-Item -ItemType Directory -Force -Path $destDir | Out-Null

        if (Test-Path -LiteralPath $dest) {
            $existing = Get-Item -LiteralPath $dest
            if ($existing.Length -eq $file.Length) {
                $skipped++
                continue
            }
        }

        Copy-Item -LiteralPath $file.FullName -Destination $dest -Force
        $copied++
    }

    Write-Host "Zip: $zipFullPath"
    Write-Host "Destination: $officialDest"
    Write-Host "Copied files: $copied"
    Write-Host "Skipped same-size files: $skipped"
    Write-Host "Done."
}
finally {
    if (Test-Path -LiteralPath $tempRoot) {
        Remove-Item -LiteralPath $tempRoot -Recurse -Force
    }
}
