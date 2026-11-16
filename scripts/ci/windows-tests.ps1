param(
    [string]$PythonScript = "scripts/ci/run-tests.py",
    [string[]]$ScriptArguments = @("--assert-outside-job")
)
$ErrorActionPreference = 'Stop'
$taskName = "mcpcli-ci-$env:GITHUB_RUN_ID-$env:GITHUB_RUN_ATTEMPT"
$log = "$env:RUNNER_TEMP\mcpcli-test.log"
$done = "$env:RUNNER_TEMP\mcpcli-test.exit"
$cmd = "$env:RUNNER_TEMP\mcpcli-test.cmd"
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
cd /d $env:GITHUB_WORKSPACE
"$python" "$PythonScript" $quotedArguments > "$log" 2>&1
echo %ERRORLEVEL% > "$done"
"@ | Set-Content -Encoding ASCII $cmd
Remove-Item $log, $done -ErrorAction SilentlyContinue
schtasks /create /tn $taskName /tr $cmd /sc once /st 00:00 /ru SYSTEM /f | Out-Null
if ($LASTEXITCODE -ne 0) { throw "could not register test task" }
try {
    schtasks /run /tn $taskName | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "could not start test task" }
    $started = Get-Date
    while (-not (Test-Path $done)) {
        if (((Get-Date) - $started).TotalSeconds -gt 2700) {
            throw "test task exceeded 45 minutes"
        }
        Start-Sleep -Seconds 5
    }
    Get-Content $log
    Copy-Item $log "$env:GITHUB_WORKSPACE\ci-test.log"
    if ([int](Get-Content $done) -ne 0) { throw "native Windows tests failed" }
} finally {
    schtasks /end /tn $taskName 2>&1 | Out-Null
    schtasks /delete /tn $taskName /f 2>&1 | Out-Null
    if (Test-Path $log) { Copy-Item $log "$env:GITHUB_WORKSPACE\ci-test.log" }
}
