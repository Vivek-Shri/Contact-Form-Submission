param(
    [string]$HostIp = "127.0.0.1",
    [int]$Port = 8000,
    [string]$App = "Back:app"
)

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$controlScript = Join-Path $root "backend-control.ps1"

if (-not (Test-Path $controlScript)) {
    Write-Error "Control script not found: $controlScript"
    exit 1
}

& powershell -ExecutionPolicy Bypass -File $controlScript -Action restart -HostIp $HostIp -Port $Port -App $App
exit $LASTEXITCODE
