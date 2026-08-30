param(
    [string]$StartMonth = "202101",
    [string]$EndMonth = "202607",
    [int]$MaxParallel = 4,
    [switch]$Force
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectDir = Split-Path -Parent $scriptDir
$python = "C:\Users\ryuou\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe"
if (-not (Test-Path -LiteralPath $python)) {
    $python = "python"
}

$outDir = Join-Path $projectDir "outputs\official_dataset_parts"
$logDir = Join-Path $projectDir "outputs\logs\official_dataset_parts"
New-Item -ItemType Directory -Force -Path $outDir | Out-Null
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

function Get-MonthEnd([int]$Year, [int]$Month) {
    return ([datetime]::new($Year, $Month, 1)).AddMonths(1).AddDays(-1)
}

function Next-Month([string]$MonthText) {
    $year = [int]$MonthText.Substring(0, 4)
    $month = [int]$MonthText.Substring(4, 2)
    return ([datetime]::new($year, $month, 1)).AddMonths(1).ToString("yyyyMM")
}

$months = New-Object System.Collections.Generic.List[string]
$m = $StartMonth
while ($m -le $EndMonth) {
    $months.Add($m)
    $m = Next-Month $m
}

$queue = New-Object System.Collections.Queue
foreach ($monthText in $months) {
    $out = Join-Path $outDir "official_race_dataset_$monthText.csv"
    if ((Test-Path -LiteralPath $out) -and ((Get-Item -LiteralPath $out).Length -gt 0) -and -not $Force) {
        Write-Host "skip existing $monthText $out"
        continue
    }
    $queue.Enqueue($monthText)
}

$running = New-Object System.Collections.Generic.List[object]
$started = 0
$finished = 0

while ($queue.Count -gt 0 -or $running.Count -gt 0) {
    while ($queue.Count -gt 0 -and $running.Count -lt $MaxParallel) {
        $monthText = [string]$queue.Dequeue()
        $year = [int]$monthText.Substring(0, 4)
        $month = [int]$monthText.Substring(4, 2)
        $startDate = ([datetime]::new($year, $month, 1)).ToString("yyyyMMdd")
        $endDate = (Get-MonthEnd $year $month).ToString("yyyyMMdd")
        $out = Join-Path $outDir "official_race_dataset_$monthText.csv"
        $log = Join-Path $logDir "official_race_dataset_$monthText.log"

        $command = @"
`$env:PYTHONPATH = '$projectDir\src'
Set-Location '$projectDir'
& '$python' '.\scripts\build_official_dataset.py' --start-date $startDate --end-date $endDate --out '$out' --progress-every 1000 *> '$log'
"@

        $process = Start-Process -FilePath "powershell" -ArgumentList @("-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", $command) -WindowStyle Hidden -PassThru
        $running.Add([pscustomobject]@{
            Month = $monthText
            Process = $process
            Out = $out
            Log = $log
            Started = Get-Date
        }) | Out-Null
        $started++
        Write-Host "started $monthText pid=$($process.Id) log=$log"
    }

    Start-Sleep -Seconds 10

    for ($i = $running.Count - 1; $i -ge 0; $i--) {
        $item = $running[$i]
        if ($item.Process.HasExited) {
            $code = $item.Process.ExitCode
            $seconds = [int]((Get-Date) - $item.Started).TotalSeconds
            $size = 0
            if (Test-Path -LiteralPath $item.Out) {
                $size = (Get-Item -LiteralPath $item.Out).Length
            }
            Write-Host "finished $($item.Month) exit=$code seconds=$seconds bytes=$size"
            $running.RemoveAt($i)
            $finished++
        }
    }
}

$manifest = Join-Path $outDir "manifest.txt"
Get-ChildItem -LiteralPath $outDir -File -Filter "official_race_dataset_*.csv" |
    Sort-Object Name |
    ForEach-Object { $_.FullName } |
    Set-Content -LiteralPath $manifest -Encoding UTF8

Write-Host "done started=$started finished=$finished manifest=$manifest"
