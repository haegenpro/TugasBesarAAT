# Python 3 is required; the generator preserves existing files and prints no secrets.
param([string]$OutputRoot = (Split-Path -Parent $PSScriptRoot))
$ErrorActionPreference = 'Stop'
python3 (Join-Path $PSScriptRoot 'setup-env.py') --output-root $OutputRoot
if ($LASTEXITCODE -ne 0) { throw 'Environment setup failed; existing files were preserved.' }
