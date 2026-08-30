param(
    [string]$BaseDir = "C:\Users\ryuou\Documents\Codex\2026-07-14\c-users-ryuou-documents-codex-2026\bortrace_data",
    [datetime]$StartDate,
    [datetime]$EndDate,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

if (-not $StartDate -or -not $EndDate) {
    throw "StartDate and EndDate are required. Example: -StartDate '2021-01-01' -EndDate '2022-12-31'"
}

$baseUrl = "https://www1.mbrace.or.jp/od2"
$lzhDir = Join-Path $BaseDir "lzh"
$txtDir = Join-Path $BaseDir "txt"
$reportName = "download_{0}_{1}_report.csv" -f $StartDate.ToString("yyyyMMdd"), $EndDate.ToString("yyyyMMdd")
$reportPath = Join-Path $BaseDir $reportName

New-Item -ItemType Directory -Force -Path $lzhDir | Out-Null
New-Item -ItemType Directory -Force -Path $txtDir | Out-Null

$results = New-Object System.Collections.Generic.List[object]
$current = $StartDate

while ($current -le $EndDate) {
    $yyyyMMdd = $current.ToString("yyyyMMdd")
    $yyMMdd = $current.ToString("yyMMdd")
    $yyyyMM = $current.ToString("yyyyMM")

    foreach ($kind in @("b", "k")) {
        $kindUpper = $kind.ToUpperInvariant()
        $lzhName = "$kind$yyMMdd.lzh"
        $txtName = "$kindUpper$yyMMdd.TXT"
        $url = "$baseUrl/$kindUpper/$yyyyMM/$lzhName"
        $lzhPath = Join-Path $lzhDir $lzhName
        $txtPath = Join-Path $txtDir $txtName

        if ((Test-Path -LiteralPath $txtPath) -and ((Get-Item -LiteralPath $txtPath).Length -gt 0)) {
            Write-Host "$yyyyMMdd $kindUpper exists"
            $results.Add([pscustomobject]@{ Date = $yyyyMMdd; Type = $kindUpper; Status = "exists"; Path = $txtPath; Url = $url })
            continue
        }

        if ($DryRun) {
            Write-Host "$yyyyMMdd $kindUpper dry-run $url"
            $results.Add([pscustomobject]@{ Date = $yyyyMMdd; Type = $kindUpper; Status = "dry-run"; Path = $txtPath; Url = $url })
            continue
        }

        try {
            Write-Host "$yyyyMMdd $kindUpper download"
            Invoke-WebRequest -Uri $url -OutFile $lzhPath -TimeoutSec 30
            tar -xf $lzhPath -C $txtDir

            if ((Test-Path -LiteralPath $txtPath) -and ((Get-Item -LiteralPath $txtPath).Length -gt 0)) {
                $results.Add([pscustomobject]@{ Date = $yyyyMMdd; Type = $kindUpper; Status = "success"; Path = $txtPath; Url = $url })
            } else {
                $results.Add([pscustomobject]@{ Date = $yyyyMMdd; Type = $kindUpper; Status = "extract_missing"; Path = $txtPath; Url = $url })
            }
        } catch {
            Write-Host "$yyyyMMdd $kindUpper error $($_.Exception.Message)"
            $results.Add([pscustomobject]@{ Date = $yyyyMMdd; Type = $kindUpper; Status = "error"; Path = $txtPath; Url = $url })
        }

        Start-Sleep -Milliseconds 150
    }

    $current = $current.AddDays(1)
}

$results | Export-Csv -LiteralPath $reportPath -NoTypeInformation -Encoding UTF8
$results | Group-Object Status | Sort-Object Name | Select-Object Name,Count | Format-Table -AutoSize
Write-Host "Report: $reportPath"
