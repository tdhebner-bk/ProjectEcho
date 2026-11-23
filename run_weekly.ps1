# Entry point for the "Project Echo Weekly" scheduled task.
#
# Installs anything missing, then runs the burndown simulation headless
# (run_simulation.py), which saves its outputs to
# OneDrive - Q2e\Project Echo Simulations. Python's stdout/stderr go to the
# log so a crash leaves a traceback rather than silence.
# Snowflake auth is externalbrowser: expect an Okta window.

$Here   = Split-Path -Parent $MyInvocation.MyCommand.Path
$Py     = 'C:\Users\thebner\PY_Analytics\.venv\Scripts\python.exe'
$LogDir = Join-Path $env:LOCALAPPDATA 'ProjectEcho\logs'
New-Item -ItemType Directory -Force $LogDir | Out-Null
$Log    = Join-Path $LogDir 'scheduled.log'

function Write-Log($msg) { "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')  $msg" | Add-Content $Log }

Write-Log "======== start ========"

# No --upgrade: the .venv is shared with Slotting and the Budget Generator,
# so already-installed packages must stay at their current versions.
& $Py -m pip install --disable-pip-version-check -q streamlit snowflake-connector-python matplotlib pandas *>> $Log
if ($LASTEXITCODE -ne 0) { Write-Log "pip install failed ($LASTEXITCODE)"; exit 1 }

Set-Location $Here
$env:PYTHONIOENCODING = 'utf-8'
& cmd /c "`"$Py`" `"$Here\run_simulation.py`" >> `"$Log`" 2>&1"
$rc = $LASTEXITCODE
Write-Log "exit code: $rc"
exit $rc
