param(
    [string]$PythonScript = "scripts/ci/run-tests.py",
    [string[]]$ScriptArguments = @("--assert-outside-job")
)
$ErrorActionPreference = 'Stop'
$taskName = "mcpcli-ci-$env:GITHUB_RUN_ID-$env:GITHUB_RUN_ATTEMPT"
$log = Join-Path $env:RUNNER_TEMP 'mcpcli-test.log'
$done = Join-Path $env:RUNNER_TEMP 'mcpcli-test.exit'
$cmd = Join-Path $env:RUNNER_TEMP 'mcpcli-test.cmd'
$rustupHome = if ($env:RUSTUP_HOME) { $env:RUSTUP_HOME } else { "$env:USERPROFILE\.rustup" }
$cargoHome = if ($env:CARGO_HOME) { $env:CARGO_HOME } else { "$env:USERPROFILE\.cargo" }
$python = (Get-Command python).Source
$quotedArguments = ($ScriptArguments | ForEach-Object { '"' + $_.Replace('"', '\"') + '"' }) -join ' ' 
@"
@echo off
set CI=true
set RUSTUP_HOME=$rustupHome
set CARGO_HOME=$cargoHome
set PATH=$cargoHome\bin;$env:PATH
set PYTHONUTF8=1
set PYTHONUNBUFFERED=1
cd /d $env:GITHUB_WORKSPACE
"$python" "$PythonScript" $quotedArguments > "$log" 2>&1
echo %ERRORLEVEL% > "$done"
"@ | Set-Content -Encoding ASCII $cmd
Remove-Item $log, $done -ErrorAction SilentlyContinue
$logReader = $null
function Write-NewLog {
    if (-not $script:logReader -and (Test-Path $log)) {
        $stream = [System.IO.File]::Open($log, 'Open', 'Read', 'ReadWrite')
        $script:logReader = [System.IO.StreamReader]::new($stream, [System.Text.Encoding]::UTF8)
    }
    if ($script:logReader) {
        $text = $script:logReader.ReadToEnd()
        if ($text) { Write-Host -NoNewline $text }
    }
}
schtasks /create /tn $taskName /tr $cmd /sc once /st 00:00 /ru SYSTEM /f | Out-Null
if ($LASTEXITCODE -ne 0) { throw "could not register test task" }
try {
    $started = Get-Date
    $lastStatus = $started.AddSeconds(-30)
    schtasks /run /tn $taskName | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "could not start test task" }
    while (-not (Test-Path $done)) {
        Write-NewLog
        $now = Get-Date
        $elapsed = ($now - $started).TotalSeconds
        $state = (Get-ScheduledTask -TaskName $taskName).State
        $info = Get-ScheduledTaskInfo -TaskName $taskName
        $result = '0x{0:X8}' -f [uint32]$info.LastTaskResult
        if (($now - $lastStatus).TotalSeconds -ge 30) {
            Write-Host "Scheduled task: State=$state LastTaskResult=$result Elapsed=$([int]$elapsed)s"
            $lastStatus = $now
        }
        $ran = (Test-Path $log) -or $info.LastRunTime -ge $started.AddSeconds(-2)
        if ($ran -and $state -notin @('Running', 'Queued') -and -not (Test-Path $done)) {
            throw "test task stopped without an exit-code file: State=$state LastTaskResult=$result"
        }
        if ($elapsed -gt 60 -and -not (Test-Path $log)) {
            throw "test task did not open its log: State=$state LastTaskResult=$result"
        }
        if ($elapsed -gt 2700) {
            throw "test task exceeded 45 minutes: State=$state LastTaskResult=$result"
        }
        Start-Sleep -Seconds 5
    }
    Write-NewLog
    if ([int](Get-Content $done) -ne 0) { throw "native Windows tests failed" }
} finally {
    schtasks /end /tn $taskName 2>&1 | Out-Null
    schtasks /delete /tn $taskName /f 2>&1 | Out-Null
    Write-NewLog
    if ($logReader) { $logReader.Dispose() }
    if (Test-Path $log) { Copy-Item $log (Join-Path $env:GITHUB_WORKSPACE 'ci-test.log') }
}
