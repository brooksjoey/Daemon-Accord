# Daemon Accord Management Script for Windows
# Usage: .\scripts\manage.ps1 <command> [options]

param(
    [Parameter(Position=0)]
    [ValidateSet("up", "down", "logs", "proof", "status", "restart", "clean")]
    [string]$Command = "status",
    
    [Parameter()]
    [switch]$Prod,
    
    [Parameter()]
    [string]$Service
)

$ErrorActionPreference = "Stop"

# Detect docker compose command
$dockerCompose = "docker compose"
if (-not (Get-Command "docker" -ErrorAction SilentlyContinue)) {
    Write-Error "Docker is not installed or not in PATH"
    exit 1
}

# Check for docker compose (new style) or docker-compose (old style)
$composeCheck = docker compose version 2>&1
if ($LASTEXITCODE -ne 0) {
    $composeCheck = docker-compose version 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Neither 'docker compose' nor 'docker-compose' is available"
        exit 1
    }
    $dockerCompose = "docker-compose"
}

# Determine compose file
$composeFile = if ($Prod) { "docker-compose.prod.yml" } else { "docker-compose.yml" }

# Get project root (assumes script is in scripts/)
$projectRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
Set-Location $projectRoot

function Show-Status {
    Write-Host "`n=== Daemon Accord Status ===" -ForegroundColor Cyan
    Write-Host "Compose file: $composeFile" -ForegroundColor Gray
    
    # Check if services are running
    $services = docker compose -f $composeFile ps --format json 2>&1 | ConvertFrom-Json
    if ($services) {
        Write-Host "`nRunning Services:" -ForegroundColor Green
        foreach ($svc in $services) {
            $status = if ($svc.State -eq "running") { "✓" } else { "✗" }
            $color = if ($svc.State -eq "running") { "Green" } else { "Red" }
            Write-Host "  $status $($svc.Service) - $($svc.State)" -ForegroundColor $color
        }
    } else {
        Write-Host "No services running" -ForegroundColor Yellow
    }
    
    # Check Control Plane health
    Write-Host "`nControl Plane Health:" -ForegroundColor Cyan
    try {
        $response = Invoke-RestMethod -Uri "http://localhost:8082/health" -TimeoutSec 2 -ErrorAction Stop
        Write-Host "  ✓ Control Plane: $($response.status)" -ForegroundColor Green
        Write-Host "    Workers: $($response.workers)" -ForegroundColor Gray
    } catch {
        Write-Host "  ✗ Control Plane: Not responding" -ForegroundColor Red
    }
    
    # Check Ops Status
    Write-Host "`nOperational Status:" -ForegroundColor Cyan
    try {
        $ops = Invoke-RestMethod -Uri "http://localhost:8082/api/v1/ops/status" -TimeoutSec 2 -ErrorAction Stop
        Write-Host "  Health: $($ops.health.status)" -ForegroundColor $(if ($ops.health.status -eq "healthy") { "Green" } else { "Yellow" })
        Write-Host "  Queue Depth: $($ops.queue.depth)" -ForegroundColor Gray
        Write-Host "  Success Rate: $($ops.metrics.success_rate_percent)%" -ForegroundColor Gray
    } catch {
        Write-Host "  ✗ Ops endpoint: Not responding" -ForegroundColor Red
    }
    
    Write-Host ""
}

function Start-Services {
    Write-Host "Starting Daemon Accord..." -ForegroundColor Cyan
    Write-Host "Using compose file: $composeFile" -ForegroundColor Gray
    
    if ($Prod) {
        Write-Host "`n⚠ Production mode (low-resource):" -ForegroundColor Yellow
        Write-Host "  - MAX_BROWSERS=1" -ForegroundColor Gray
        Write-Host "  - WORKER_COUNT=1" -ForegroundColor Gray
        Write-Host "  - MAX_CONCURRENT_JOBS=10" -ForegroundColor Gray
    }
    
    docker compose -f $composeFile up -d --build
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "`n✓ Services started" -ForegroundColor Green
        Write-Host "Waiting for services to be healthy..." -ForegroundColor Gray
        Start-Sleep -Seconds 5
        Show-Status
    } else {
        Write-Error "Failed to start services"
        exit 1
    }
}

function Stop-Services {
    Write-Host "Stopping Daemon Accord..." -ForegroundColor Cyan
    docker compose -f $composeFile down
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✓ Services stopped" -ForegroundColor Green
    } else {
        Write-Error "Failed to stop services"
        exit 1
    }
}

function Show-Logs {
    if ($Service) {
        Write-Host "Showing logs for: $Service" -ForegroundColor Cyan
        docker compose -f $composeFile logs -f $Service
    } else {
        Write-Host "Showing logs for all services (Ctrl+C to exit)" -ForegroundColor Cyan
        docker compose -f $composeFile logs -f
    }
}

function Run-ProofPack {
    Write-Host "Running Production Proof Pack..." -ForegroundColor Cyan
    Write-Host "This will validate end-to-end functionality" -ForegroundColor Gray
    
    if (-not (Test-Path "scripts\proof_pack\run_proof_pack.py")) {
        Write-Error "Proof pack script not found"
        exit 1
    }
    
    python scripts\proof_pack\run_proof_pack.py
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "`n✓ Proof pack completed successfully" -ForegroundColor Green
    } else {
        Write-Error "Proof pack failed"
        exit 1
    }
}

function Restart-Services {
    Write-Host "Restarting Daemon Accord..." -ForegroundColor Cyan
    Stop-Services
    Start-Sleep -Seconds 2
    Start-Services
}

function Clean-Resources {
    Write-Host "Cleaning up Docker resources..." -ForegroundColor Cyan
    Write-Host "This will remove containers, volumes, and images" -ForegroundColor Yellow
    
    $confirm = Read-Host "Are you sure? (yes/no)"
    if ($confirm -ne "yes") {
        Write-Host "Cancelled" -ForegroundColor Yellow
        return
    }
    
    docker compose -f $composeFile down -v --rmi all
    Write-Host "✓ Cleanup complete" -ForegroundColor Green
}

# Main command router
switch ($Command) {
    "up" { Start-Services }
    "down" { Stop-Services }
    "logs" { Show-Logs }
    "proof" { Run-ProofPack }
    "status" { Show-Status }
    "restart" { Restart-Services }
    "clean" { Clean-Resources }
    default {
        Write-Host "Unknown command: $Command" -ForegroundColor Red
        Write-Host "`nAvailable commands:" -ForegroundColor Cyan
        Write-Host "  up       - Start services (dev mode)"
        Write-Host "  up -Prod - Start services (production mode)"
        Write-Host "  down     - Stop services"
        Write-Host "  logs     - Show logs (all services)"
        Write-Host "  logs -Service <name> - Show logs for specific service"
        Write-Host "  proof    - Run production proof pack"
        Write-Host "  status   - Show system status"
        Write-Host "  restart  - Restart all services"
        Write-Host "  clean    - Remove all containers, volumes, and images"
        exit 1
    }
}

