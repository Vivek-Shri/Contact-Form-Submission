param(
    [ValidateSet("start", "stop", "restart", "status")]
    [string]$Action = "status",
    [string]$HostIp = "127.0.0.1",
    [int]$Port = 8000,
    [string]$App = "Back:app",
    [switch]$Force
)

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$python = Join-Path $root ".venv\Scripts\python.exe"

function Get-PortListeners {
    param([int]$TargetPort)

    $rows = Get-NetTCPConnection -LocalPort $TargetPort -State Listen -ErrorAction SilentlyContinue |
        Select-Object -ExpandProperty OwningProcess -Unique

    if (-not $rows) {
        return @()
    }

    $items = @()
    foreach ($procId in $rows) {
        $proc = Get-Process -Id $procId -ErrorAction SilentlyContinue
        $cmd = (Get-CimInstance Win32_Process -Filter "ProcessId=$procId" -ErrorAction SilentlyContinue).CommandLine
        $items += [pscustomobject]@{
            Id = $procId
            Name = if ($proc) { $proc.ProcessName } else { "unknown" }
            CommandLine = $cmd
        }
    }

    return $items
}

function Test-BackendHealth {
    param([string]$TargetHost, [int]$TargetPort)

    try {
        $response = Invoke-WebRequest -UseBasicParsing -Uri "http://$TargetHost`:$TargetPort/health" -TimeoutSec 3
        return [pscustomobject]@{
            Ok = $true
            Body = $response.Content
        }
    }
    catch {
        return [pscustomobject]@{
            Ok = $false
            Body = $_.Exception.Message
        }
    }
}

if (-not (Test-Path $python)) {
    Write-Error "Python executable not found: $python"
    exit 1
}

$listeners = Get-PortListeners -TargetPort $Port

switch ($Action) {
    "status" {
        if (-not $listeners) {
            Write-Host "Backend status: stopped (no listener on $HostIp`:$Port)"
            exit 0
        }

        Write-Host "Backend status: running on $HostIp`:$Port"
        foreach ($item in $listeners) {
            Write-Host "PID=$($item.Id) NAME=$($item.Name)"
            Write-Host "CMD=$($item.CommandLine)"
        }

        $health = Test-BackendHealth -TargetHost $HostIp -TargetPort $Port
        if ($health.Ok) {
            Write-Host "HEALTH=$($health.Body)"
            exit 0
        }

        Write-Warning "Health check failed: $($health.Body)"
        exit 1
    }

    "stop" {
        if (-not $listeners) {
            Write-Host "No process is listening on $HostIp`:$Port"
            exit 0
        }

        foreach ($item in $listeners) {
            try {
                Write-Host "Stopping PID $($item.Id) ($($item.Name)) on port $Port..."
                Stop-Process -Id $item.Id -Force
            }
            catch {
                Write-Warning "Could not stop PID $($item.Id): $($_.Exception.Message)"
            }
        }

        $remaining = Get-PortListeners -TargetPort $Port
        if ($remaining) {
            Write-Error "Port $Port is still in use after stop attempt."
            exit 1
        }

        Write-Host "Stopped. Port $Port is free."
        exit 0
    }

    "start" {
        if ($listeners -and -not $Force) {
            Write-Host "Backend already running on $HostIp`:$Port"
            foreach ($item in $listeners) {
                Write-Host "PID=$($item.Id) NAME=$($item.Name)"
                Write-Host "CMD=$($item.CommandLine)"
            }
            Write-Host "Use -Action restart or add -Force with -Action start if you want to replace it."
            exit 0
        }

        if ($listeners -and $Force) {
            foreach ($item in $listeners) {
                try {
                    Write-Host "Force stopping PID $($item.Id) ($($item.Name)) on port $Port..."
                    Stop-Process -Id $item.Id -Force
                }
                catch {
                    Write-Warning "Could not stop PID $($item.Id): $($_.Exception.Message)"
                }
            }
        }

        $args = @("-m", "uvicorn", $App, "--host", $HostIp, "--port", "$Port")
        $proc = Start-Process -FilePath $python -ArgumentList $args -WorkingDirectory $root -PassThru
        Write-Host "Started backend PID $($proc.Id) on http://$HostIp`:$Port"

        $healthOk = $false
        for ($i = 0; $i -lt 15; $i++) {
            Start-Sleep -Milliseconds 300
            $health = Test-BackendHealth -TargetHost $HostIp -TargetPort $Port
            if ($health.Ok) {
                $healthOk = $true
                Write-Host "HEALTH=$($health.Body)"
                break
            }
        }

        if (-not $healthOk) {
            Write-Warning "Backend process started but health check is not ready yet."
            exit 1
        }

        exit 0
    }

    "restart" {
        foreach ($item in $listeners) {
            try {
                Write-Host "Stopping PID $($item.Id) ($($item.Name)) on port $Port..."
                Stop-Process -Id $item.Id -Force
            }
            catch {
                Write-Warning "Could not stop PID $($item.Id): $($_.Exception.Message)"
            }
        }

        $remaining = Get-PortListeners -TargetPort $Port
        if ($remaining) {
            Write-Error "Port $Port is still in use; restart aborted."
            exit 1
        }

        $args = @("-m", "uvicorn", $App, "--host", $HostIp, "--port", "$Port")
        $proc = Start-Process -FilePath $python -ArgumentList $args -WorkingDirectory $root -PassThru
        Write-Host "Restarted backend PID $($proc.Id) on http://$HostIp`:$Port"

        $healthOk = $false
        for ($i = 0; $i -lt 15; $i++) {
            Start-Sleep -Milliseconds 300
            $health = Test-BackendHealth -TargetHost $HostIp -TargetPort $Port
            if ($health.Ok) {
                $healthOk = $true
                Write-Host "HEALTH=$($health.Body)"
                break
            }
        }

        if (-not $healthOk) {
            Write-Warning "Backend process restarted but health check is not ready yet."
            exit 1
        }

        exit 0
    }
}
