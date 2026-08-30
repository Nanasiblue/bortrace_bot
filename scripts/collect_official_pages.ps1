param(
    [string]$BaseDir = "C:\Users\ryuou\Documents\Codex\2026-07-14\c-users-ryuou-documents-codex-2026\bortrace_data",
    [datetime]$StartDate,
    [datetime]$EndDate,
    [string[]]$Pages = @("racelist", "beforeinfo", "odds3t", "odds3f", "odds2tf", "oddsk", "oddstf", "raceresult"),
    [int]$SleepMs = 200,
    [int]$TimeoutSec = 30,
    [int]$RetryCount = 2,
    [int]$ProgressEvery = 100,
    [int]$TimingEvery = 100,
    [switch]$FastExistingCheck,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

if (-not $StartDate -or -not $EndDate) {
    throw "StartDate and EndDate are required. Example: -StartDate '2021-01-01' -EndDate '2026-07-14'"
}

$rawRoot = Join-Path $BaseDir "official_pages"
$reportName = "official_pages_{0}_{1}_report.csv" -f $StartDate.ToString("yyyyMMdd"), $EndDate.ToString("yyyyMMdd")
$reportPath = Join-Path $BaseDir $reportName
$baseUrl = "https://www.boatrace.jp/owpc/pc/race"
$results = New-Object System.Collections.Generic.List[object]
$savedCount = 0
$existsCount = 0
$errorCount = 0
$timingCount = 0
$timingHttpMs = 0.0
$timingValidateMs = 0.0
$timingExistingMs = 0.0
$timingHttpCount = 0
$timingValidateCount = 0
$timingExistingCount = 0

function Add-Timing {
    param(
        [double]$HttpMs = 0.0,
        [double]$ValidateMs = 0.0,
        [double]$ExistingMs = 0.0
    )

    if ($TimingEvery -le 0) {
        return
    }

    $script:timingCount += 1
    if ($HttpMs -gt 0) {
        $script:timingHttpMs += $HttpMs
        $script:timingHttpCount += 1
    }
    if ($ValidateMs -gt 0) {
        $script:timingValidateMs += $ValidateMs
        $script:timingValidateCount += 1
    }
    if ($ExistingMs -gt 0) {
        $script:timingExistingMs += $ExistingMs
        $script:timingExistingCount += 1
    }

    if ($script:timingCount % $TimingEvery -eq 0) {
        $avgHttp = 0
        $avgValidate = 0
        $avgExisting = 0
        if ($script:timingHttpCount -gt 0) {
            $avgHttp = [Math]::Round($script:timingHttpMs / $script:timingHttpCount, 1)
        }
        if ($script:timingValidateCount -gt 0) {
            $avgValidate = [Math]::Round($script:timingValidateMs / $script:timingValidateCount, 1)
        }
        if ($script:timingExistingCount -gt 0) {
            $avgExisting = [Math]::Round($script:timingExistingMs / $script:timingExistingCount, 1)
        }
        Write-Host "timing count=$script:timingCount http_count=$script:timingHttpCount avg_http_ms=$avgHttp validate_count=$script:timingValidateCount avg_validate_ms=$avgValidate existing_count=$script:timingExistingCount avg_existing_ms=$avgExisting"
    }
}

New-Item -ItemType Directory -Force -Path $rawRoot | Out-Null

Add-Type -AssemblyName System.Net.Http
$handler = New-Object System.Net.Http.HttpClientHandler
$handler.AutomaticDecompression = [System.Net.DecompressionMethods]::GZip -bor [System.Net.DecompressionMethods]::Deflate
$httpClient = [System.Net.Http.HttpClient]::new($handler)
$httpClient.Timeout = [TimeSpan]::FromSeconds($TimeoutSec)
$httpClient.DefaultRequestHeaders.UserAgent.ParseAdd("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
$httpClient.DefaultRequestHeaders.Accept.ParseAdd("text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")

function Save-HttpUrl {
    param(
        [string]$Url,
        [string]$Path
    )

    $tmpPath = "$Path.tmp"
    if (Test-Path -LiteralPath $tmpPath) {
        Remove-Item -LiteralPath $tmpPath -Force
    }

    $lastError = $null
    for ($attempt = 0; $attempt -le $RetryCount; $attempt++) {
        try {
            $sw = [System.Diagnostics.Stopwatch]::StartNew()
            $response = $httpClient.GetAsync($Url).GetAwaiter().GetResult()
            if (-not $response.IsSuccessStatusCode) {
                throw "HTTP $([int]$response.StatusCode) $($response.ReasonPhrase)"
            }

            $bytes = $response.Content.ReadAsByteArrayAsync().GetAwaiter().GetResult()
            [System.IO.File]::WriteAllBytes($tmpPath, $bytes)
            $sw.Stop()
            return $sw.Elapsed.TotalMilliseconds
        } catch {
            if (Test-Path -LiteralPath $tmpPath) {
                Remove-Item -LiteralPath $tmpPath -Force
            }
            $lastError = $_.Exception.Message
            if ($attempt -ge $RetryCount) {
                throw $lastError
            }

            $backoffMs = [Math]::Min(30000, 2000 * [Math]::Pow(2, $attempt))
            Write-Host "retry wait ${backoffMs}ms $Url"
            Start-Sleep -Milliseconds $backoffMs
        }
    }
}

function Commit-TempHtml {
    param(
        [string]$Path
    )

    $tmpPath = "$Path.tmp"
    if (-not (Test-Path -LiteralPath $tmpPath)) {
        return $false
    }

    if (Test-Path -LiteralPath $Path) {
        Remove-Item -LiteralPath $Path -Force
    }

    Move-Item -LiteralPath $tmpPath -Destination $Path -Force
    return $true
}

function Remove-TempHtml {
    param(
        [string]$Path
    )

    $tmpPath = "$Path.tmp"
    if (Test-Path -LiteralPath $tmpPath) {
        Remove-Item -LiteralPath $tmpPath -Force
    }
}

function Normalize-Pages {
    param([string[]]$RawPages)

    $normalized = @()
    foreach ($page in $RawPages) {
        foreach ($part in ($page -split ",")) {
            $trimmed = $part.Trim()
            if ($trimmed) {
                $normalized += $trimmed
            }
        }
    }
    return $normalized | Select-Object -Unique
}

function Test-OfficialHtml {
    param(
        [string]$Path,
        [string]$Kind
    )

    if (-not (Test-Path -LiteralPath $Path)) {
        return $false
    }

    $item = Get-Item -LiteralPath $Path
    if ($item.Length -le 0) {
        return $false
    }

    $html = Get-Content -LiteralPath $Path -Raw -Encoding UTF8
    $errorPatterns = @(
        "システムエラー",
        "予期せぬエラーが発生しました",
        "お探しのページは見つかりません",
        "Not Found",
        "Service Unavailable"
    )

    foreach ($pattern in $errorPatterns) {
        if ($html.Contains($pattern)) {
            return $false
        }
    }

    return $true
}

$Pages = Normalize-Pages -RawPages $Pages

function Save-Url {
    param(
        [string]$Url,
        [string]$Path,
        [string]$Kind,
        [string]$Date,
        [string]$Jcd,
        [string]$Rno
    )

    if (Test-Path -LiteralPath $Path) {
        $existingSw = [System.Diagnostics.Stopwatch]::StartNew()
        $existingOk = $false
        if ($FastExistingCheck) {
            $existingOk = ((Get-Item -LiteralPath $Path).Length -gt 1000)
        } else {
            $existingOk = Test-OfficialHtml -Path $Path -Kind $Kind
        }

        if ($existingOk) {
            $existingSw.Stop()
            Add-Timing -ExistingMs $existingSw.Elapsed.TotalMilliseconds
            $script:existsCount += 1
            if (($script:existsCount + $script:savedCount + $script:errorCount) % $ProgressEvery -eq 0) {
                Write-Host "progress saved=$script:savedCount exists=$script:existsCount errors=$script:errorCount latest=$Date jcd=$Jcd rno=$Rno $Kind"
            }
            $results.Add([pscustomobject]@{ Date = $Date; Jcd = $Jcd; Rno = $Rno; Kind = $Kind; Status = "exists"; Path = $Path; Url = $Url })
            return $true
        }

        $existingSw.Stop()
        Add-Timing -ExistingMs $existingSw.Elapsed.TotalMilliseconds
        Write-Host "$Date jcd=$Jcd rno=$Rno $Kind invalid existing"
        Remove-Item -LiteralPath $Path -Force
    }

    if ($DryRun) {
        Write-Host "$Date jcd=$Jcd rno=$Rno $Kind dry-run $Url"
        $results.Add([pscustomobject]@{ Date = $Date; Jcd = $Jcd; Rno = $Rno; Kind = $Kind; Status = "dry-run"; Path = $Path; Url = $Url })
        return $true
    }

    try {
        New-Item -ItemType Directory -Force -Path (Split-Path -Parent $Path) | Out-Null
        $httpMs = Save-HttpUrl -Url $Url -Path $Path
        $tmpPath = "$Path.tmp"
        $validateSw = [System.Diagnostics.Stopwatch]::StartNew()
        if (Test-OfficialHtml -Path $tmpPath -Kind $Kind) {
            $validateSw.Stop()
            Add-Timing -HttpMs $httpMs -ValidateMs $validateSw.Elapsed.TotalMilliseconds
            Commit-TempHtml -Path $Path | Out-Null
            $script:savedCount += 1
            if (($script:existsCount + $script:savedCount + $script:errorCount) % $ProgressEvery -eq 0) {
                Write-Host "progress saved=$script:savedCount exists=$script:existsCount errors=$script:errorCount latest=$Date jcd=$Jcd rno=$Rno $Kind"
            }
            $results.Add([pscustomobject]@{ Date = $Date; Jcd = $Jcd; Rno = $Rno; Kind = $Kind; Status = "success"; Path = $Path; Url = $Url })
            Start-Sleep -Milliseconds $SleepMs
            return $true
        }

        $validateSw.Stop()
        Add-Timing -HttpMs $httpMs -ValidateMs $validateSw.Elapsed.TotalMilliseconds
        Remove-TempHtml -Path $Path

        Write-Host "$Date jcd=$Jcd rno=$Rno $Kind invalid_html"
        $results.Add([pscustomobject]@{ Date = $Date; Jcd = $Jcd; Rno = $Rno; Kind = $Kind; Status = "invalid_html"; Path = $Path; Url = $Url })
        return $false
    } catch {
        $script:errorCount += 1
        Write-Host "$Date jcd=$Jcd rno=$Rno $Kind error $($_.Exception.Message)"
        $results.Add([pscustomobject]@{ Date = $Date; Jcd = $Jcd; Rno = $Rno; Kind = $Kind; Status = "error"; Path = $Path; Url = $Url })
        Start-Sleep -Milliseconds $SleepMs
        return $false
    }
}

function Get-ActiveVenues {
    param([string]$Date)

    if ($DryRun) {
        return 1..24 | ForEach-Object { $_.ToString("00") }
    }

    $indexDir = Join-Path $rawRoot "$Date\index"
    $indexPath = Join-Path $indexDir "index.html"
    $indexUrl = "$baseUrl/index?hd=$Date"
    $savedIndex = Save-Url -Url $indexUrl -Path $indexPath -Kind "index" -Date $Date -Jcd "" -Rno ""

    if (-not $savedIndex -or -not (Test-Path -LiteralPath $indexPath)) {
        return $null
    }

    $html = Get-Content -LiteralPath $indexPath -Raw -Encoding UTF8
    $matches = [regex]::Matches($html, "jcd=(\d{2})")
    $venues = @()
    foreach ($m in $matches) {
        $venues += $m.Groups[1].Value
    }
    return $venues | Sort-Object -Unique
}

function Get-RaceNumbers {
    param(
        [string]$Date,
        [string]$Jcd
    )

    if ($DryRun) {
        return 1..12
    }

    $raceIndexDir = Join-Path $rawRoot "$Date\$Jcd"
    $raceIndexPath = Join-Path $raceIndexDir "raceindex.html"
    $raceIndexUrl = "$baseUrl/raceindex?jcd=$Jcd&hd=$Date"
    Save-Url -Url $raceIndexUrl -Path $raceIndexPath -Kind "raceindex" -Date $Date -Jcd $Jcd -Rno "" | Out-Null

    if (-not (Test-Path -LiteralPath $raceIndexPath)) {
        return @()
    }

    $html = Get-Content -LiteralPath $raceIndexPath -Raw -Encoding UTF8
    $matches = [regex]::Matches($html, "rno=(\d{1,2})")
    $races = @()
    foreach ($m in $matches) {
        $rno = [int]$m.Groups[1].Value
        if ($rno -ge 1 -and $rno -le 12) {
            $races += $rno
        }
    }

    $uniqueRaces = $races | Sort-Object -Unique
    if ($uniqueRaces.Count -eq 0) {
        return 1..12
    }
    return $uniqueRaces
}

$current = $StartDate
while ($current -le $EndDate) {
    $date = $current.ToString("yyyyMMdd")
    $venues = Get-ActiveVenues -Date $date

    if ($null -eq $venues) {
        Write-Host "$date index unavailable; skip day for retry"
        $current = $current.AddDays(1)
        continue
    }

    if ($venues.Count -eq 0) {
        Write-Host "$date no active venues found"
        $results.Add([pscustomobject]@{ Date = $date; Jcd = ""; Rno = ""; Kind = "day"; Status = "no_venues"; Path = ""; Url = "$baseUrl/index?hd=$date" })
        $current = $current.AddDays(1)
        continue
    }

    foreach ($jcd in $venues) {
        $races = Get-RaceNumbers -Date $date -Jcd $jcd
        foreach ($rno in $races) {
            foreach ($page in $Pages) {
                $url = "$baseUrl/${page}?hd=$date&jcd=$jcd&rno=$rno"
                $path = Join-Path $rawRoot "$date\$jcd\$rno\$page.html"
                Save-Url -Url $url -Path $path -Kind $page -Date $date -Jcd $jcd -Rno ([string]$rno) | Out-Null
            }
        }
    }

    $current = $current.AddDays(1)
}

$results | Export-Csv -LiteralPath $reportPath -NoTypeInformation -Encoding UTF8
$results | Group-Object Status | Sort-Object Name | Select-Object Name,Count | Format-Table -AutoSize
Write-Host "Report: $reportPath"
