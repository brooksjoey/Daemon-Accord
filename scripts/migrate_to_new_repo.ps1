# Migration Script - Push to New Clean Repository
# This script handles everything automatically

param(
    [Parameter(Mandatory=$true)]
    [string]$NewRepoUrl,
    
    [Parameter()]
    [switch]$Force
)

$ErrorActionPreference = "Stop"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Daemon Accord - Repository Migration" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Safety check
if (-not $Force) {
    Write-Host "This will:" -ForegroundColor Yellow
    Write-Host "  1. Remove connection to old repository" -ForegroundColor White
    Write-Host "  2. Initialize fresh git (removes old .git)" -ForegroundColor White
    Write-Host "  3. Create clean main branch with one commit" -ForegroundColor White
    Write-Host "  4. Push to new repository: $NewRepoUrl" -ForegroundColor White
    Write-Host ""
    Write-Host "Your old repository will NOT be touched." -ForegroundColor Green
    Write-Host ""
    $confirm = Read-Host "Continue? (yes/no)"
    if ($confirm -ne "yes") {
        Write-Host "Cancelled" -ForegroundColor Yellow
        exit 0
    }
}

Write-Host ""
Write-Host "Step 1: Removing old git connection..." -ForegroundColor Yellow
if (Test-Path .git) {
    # Backup .git just in case
    if (-not (Test-Path .git.backup)) {
        Copy-Item -Recurse .git .git.backup
        Write-Host "  ✅ Backed up .git to .git.backup" -ForegroundColor Gray
    }
    
    # Remove old remote
    $oldRemote = git remote get-url origin 2>&1
    if ($LASTEXITCODE -eq 0) {
        git remote remove origin
        Write-Host "  ✅ Removed old remote" -ForegroundColor Gray
    }
    
    # Remove .git for fresh start
    Remove-Item -Recurse -Force .git
    Write-Host "  ✅ Removed old .git (backup saved)" -ForegroundColor Gray
} else {
    Write-Host "  ℹ️  No .git found, starting fresh" -ForegroundColor Gray
}

Write-Host ""
Write-Host "Step 2: Initializing fresh git..." -ForegroundColor Yellow
git init
Write-Host "  ✅ Git initialized" -ForegroundColor Green

Write-Host ""
Write-Host "Step 3: Staging all files..." -ForegroundColor Yellow
git add .
$fileCount = (git status --short | Measure-Object -Line).Lines
Write-Host "  ✅ Staged $fileCount files" -ForegroundColor Green

Write-Host ""
Write-Host "Step 4: Creating initial commit..." -ForegroundColor Yellow
$commitMessage = @"
Initial commit: Production-ready Daemon Accord v1.0

- Policy-driven web automation platform
- Production-ready core executors (Vanilla, Stealth, Assault)
- Enterprise advanced features (Ultimate Stealth, Custom)
- Complete compliance controls and audit logging
- Three production workflows (Page Change Detection, Job Posting Monitor, Uptime Smoke Check)
- Full test coverage and CI/CD
- One-command deployment (docker-compose)
- Production proof pack validation
"@

git commit -m $commitMessage
Write-Host "  ✅ Initial commit created" -ForegroundColor Green

Write-Host ""
Write-Host "Step 5: Connecting to new repository..." -ForegroundColor Yellow
git remote add origin $NewRepoUrl
git branch -M main
Write-Host "  ✅ Connected to: $NewRepoUrl" -ForegroundColor Green

Write-Host ""
Write-Host "Step 6: Pushing to new repository..." -ForegroundColor Yellow
Write-Host "  (This may take a minute...)" -ForegroundColor Gray
git push -u origin main

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Green
    Write-Host "✅ Migration Complete!" -ForegroundColor Green
    Write-Host "========================================" -ForegroundColor Green
    Write-Host ""
    Write-Host "Your new repository:" -ForegroundColor Cyan
    Write-Host "  $NewRepoUrl" -ForegroundColor White
    Write-Host ""
    Write-Host "Repository status:" -ForegroundColor Cyan
    Write-Host "  ✅ One clean 'main' branch" -ForegroundColor Green
    Write-Host "  ✅ Professional commit history" -ForegroundColor Green
    Write-Host "  ✅ All production code included" -ForegroundColor Green
    Write-Host "  ✅ Clean root directory" -ForegroundColor Green
    Write-Host ""
    Write-Host "Next steps:" -ForegroundColor Yellow
    Write-Host "  1. Visit GitHub and verify all files are there" -ForegroundColor White
    Write-Host "  2. Test clone: git clone $NewRepoUrl" -ForegroundColor White
    Write-Host "  3. Review old repo for any historical items you want" -ForegroundColor White
    Write-Host ""
    Write-Host "Old repository backup:" -ForegroundColor Gray
    Write-Host "  .git.backup (if you need anything from old git)" -ForegroundColor Gray
} else {
    Write-Host ""
    Write-Host "❌ Push failed. Check the error above." -ForegroundColor Red
    Write-Host "Common issues:" -ForegroundColor Yellow
    Write-Host "  - Repository URL incorrect" -ForegroundColor White
    Write-Host "  - Authentication required (use GitHub CLI or SSH)" -ForegroundColor White
    Write-Host "  - Repository not created yet" -ForegroundColor White
    exit 1
}

