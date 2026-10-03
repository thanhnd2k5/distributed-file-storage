param([string]$PythonPath = "")
$ErrorActionPreference = "Stop"
$backendRoot = Split-Path -Parent $PSScriptRoot
$repoRoot = Split-Path -Parent $backendRoot
Push-Location $backendRoot
try {
    if (-not $PythonPath) {
        $PythonPath = (& py -3.12 -c "import sys; print(sys.executable)")
        if ($LASTEXITCODE -ne 0) { throw "Python 3.12 unavailable; provide -PythonPath." }
    }
    & $PythonPath -c "import sys; assert sys.version_info[:2] == (3, 12), 'Python 3.12 required'"
    if ($LASTEXITCODE -ne 0) { throw "Use Python 3.12." }
    if (-not (Test-Path -LiteralPath '.venv\Scripts\python.exe')) {
        & $PythonPath -m venv --without-pip .venv
        if ($LASTEXITCODE -ne 0) { throw "Could not create .venv." }
    }
    $venvPython = Join-Path $backendRoot '.venv\Scripts\python.exe'
    & $venvPython -c "import sys; assert sys.version_info[:2] == (3, 12)"
    if ($LASTEXITCODE -ne 0) { throw "Existing .venv does not use Python 3.12." }
    if (-not (Test-Path -LiteralPath '.venv\Lib\site-packages\pip')) {
        & $PythonPath -m pip --python .venv install pip
        if ($LASTEXITCODE -ne 0) { throw "Could not bootstrap pip into .venv." }
    }
    & $venvPython -m pip install -r requirements-dev.txt
    if ($LASTEXITCODE -ne 0) { throw "Dependency installation failed." }
    & $venvPython scripts/generate_proto.py
    if ($LASTEXITCODE -ne 0) { throw "Proto generation failed." }
    $deployEnv = Join-Path $repoRoot 'deploy\.env'
    if (-not (Test-Path -LiteralPath $deployEnv)) {
        Copy-Item -LiteralPath (Join-Path $repoRoot 'deploy\.env.example') -Destination $deployEnv
    }
    Write-Host 'Base setup complete. See README.md for Docker startup and verification.'
} finally {
    Pop-Location
}

