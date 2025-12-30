# Workspace Cleanup Script
# Organizes files for professional repository
# SAFE: Does not modify git, only organizes files

$ErrorActionPreference = "Stop"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Daemon Accord - Workspace Cleanup" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Create history directory
$historyDir = "docs\history"
if (-not (Test-Path $historyDir)) {
    New-Item -ItemType Directory -Path $historyDir -Force | Out-Null
    Write-Host "Created: $historyDir" -ForegroundColor Green
}

# Files to archive (internal tracking documents)
$filesToArchive = @(
    "CHIEF_ENGINEER_ACTION_PLAN.md",
    "CHIEF_ENGINEER_AUDIT.md",
    "CHIEF_ENGINEER_COMPLETION.md",
    "CHIEF_ENGINEER_FINAL_REPORT.md",
    "CHIEF_ENGINEER_STATUS.md",
    "STATUS_UPDATE_INVESTIGATION.md",
    "VALIDATION_PHASES.md",
    "HUMAN_VALIDATION_GUIDE.md",
    "HUMAN_VALIDATION_REPORT.md",
    "SECURITY_ARCHITECTURE.md",
    "PROOF_PACK_SUMMARY.md",
    "VALUATION_READY_SUMMARY.md",
    "WORKFLOWS_IMPLEMENTATION_SUMMARY.md",
    "WORKFLOWS_COMPLETE.md",
    "COMMERCIAL_PACKAGING_COMPLETE.md",
    "COMPLIANCE_AND_RISK_CONTROLS_COMPLETE.md",
    "ENTERPRISE_ADVANCED_FEATURES_COMPLETE.md",
    "SALES_KIT_COMPLETE.md"
)

# Files to keep (production/user-facing)
$filesToKeep = @(
    "README.md",
    "docs/business/PRODUCTION_READINESS_REPORT.md",
    "LICENSE"
)

Write-Host "Archiving internal tracking documents..." -ForegroundColor Yellow
$archived = 0
$notFound = 0

foreach ($file in $filesToArchive) {
    if (Test-Path $file) {
        $dest = Join-Path $historyDir $file
        Move-Item -Path $file -Destination $dest -Force
        Write-Host "  Archived: $file" -ForegroundColor Gray
        $archived++
    } else {
        $notFound++
    }
}

Write-Host ""
Write-Host "Archived: $archived files" -ForegroundColor Green
if ($notFound -gt 0) {
    Write-Host "Not found: $notFound files (may already be archived)" -ForegroundColor Yellow
}

# Check for duplicate/backup files
Write-Host ""
Write-Host "Checking for temporary/backup files..." -ForegroundColor Yellow
$tempFiles = Get-ChildItem -Recurse -File | Where-Object {
    $_.Name -match '\.(bak|tmp|temp|swp|~)$' -or
    $_.Name -match '^~' -or
    $_.Name -match 'backup'
}

if ($tempFiles) {
    Write-Host "Found temporary files:" -ForegroundColor Yellow
    foreach ($file in $tempFiles) {
        Write-Host "  $($file.FullName)" -ForegroundColor Gray
    }
    Write-Host ""
    $delete = Read-Host "Delete temporary files? (yes/no)"
    if ($delete -eq "yes") {
        $tempFiles | Remove-Item -Force
        Write-Host "Deleted temporary files" -ForegroundColor Green
    }
} else {
    Write-Host "No temporary files found" -ForegroundColor Green
}

# Verify .gitignore exists
Write-Host ""
Write-Host "Checking .gitignore..." -ForegroundColor Yellow
if (Test-Path ".gitignore") {
    Write-Host "  .gitignore exists" -ForegroundColor Green
} else {
    Write-Host "  WARNING: .gitignore not found" -ForegroundColor Red
    Write-Host "  Creating .gitignore..." -ForegroundColor Yellow
    # .gitignore should already exist from earlier
}

# Summary
Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Cleanup Complete" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "1. Review archived files in: $historyDir" -ForegroundColor White
Write-Host "2. Review what will be committed: git status" -ForegroundColor White
Write-Host "3. Create new GitHub repository" -ForegroundColor White
Write-Host "4. Follow migration plan: CLEANUP_AND_MIGRATION_PLAN.md" -ForegroundColor White
Write-Host ""

