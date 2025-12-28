# Cleanup and Migration Plan

**Safe path to clean, professional repository**

---

## Current Situation

- ✅ **Current workspace:** Clean, production-ready code
- ⚠️ **Existing GitHub repo:** In disarray (learning project)
- 🎯 **Goal:** Push clean state without losing historical value

---

## Strategy: Clean Workspace → New Repo → Preserve History

**Why this approach:**
1. **Safe:** Current workspace stays untouched
2. **Clean:** New repo starts fresh with perfect state
3. **Preservable:** Old repo remains for historical review
4. **Professional:** Clean commit history from day one

---

## Phase 1: Clean Up Current Workspace

### Step 1.1: Organize Documentation Files

**Keep (Production/User-Facing):**
- `README.md` - Main project overview
- `docs/reports/PRODUCTION_READINESS_REPORT.md` - Buyer-facing readiness
- `docs/` - All documentation
- `sales/` - Sales materials
- Component READMEs (in each folder)

**Archive (Internal/Historical):**
- `*_COMPLETE.md` - Move to `docs/history/` or delete
- `*_STATUS.md` - Move to `docs/history/` or delete
- `CHIEF_ENGINEER_*.md` - Move to `docs/history/` or delete
- `*_SUMMARY.md` - Review, keep valuable ones, archive rest

**Decision:** Keep essential, archive internal tracking docs

### Step 1.2: Create Archive Directory

Create `docs/history/` for internal tracking documents that might have value but aren't user-facing.

### Step 1.3: Verify .gitignore

Ensure all temporary files, artifacts, and build outputs are ignored.

---

## Phase 2: Create New Repository

### Step 2.1: Create New GitHub Repo

1. Go to GitHub
2. Create new repository: `accord-engine` (or your preferred name)
3. **DO NOT** initialize with README, .gitignore, or license (we have these)
4. Copy the repository URL

### Step 2.2: Initialize Clean Git in Current Workspace

**Option A: Fresh Start (Recommended)**
```powershell
# Remove existing .git (backup first if needed)
# Then initialize fresh
git init
git add .
git commit -m "Initial commit: Production-ready Accord Engine v1.0"
```

**Option B: Keep Current Git, Clean History**
```powershell
# Create orphan branch (clean history)
git checkout --orphan clean-main
git add .
git commit -m "Initial commit: Production-ready Accord Engine v1.0"
```

### Step 2.3: Connect to New Repo

```powershell
git remote add origin <NEW_REPO_URL>
git branch -M main
git push -u origin main
```

---

## Phase 3: Preserve Historical Value

### Step 3.1: Review Old Repository

After new repo is pushed, review old repository:
- Look for valuable historical commits
- Identify important files not in current workspace
- Note any useful context or decisions

### Step 3.2: Manual Preservation

If you find valuable historical items:
1. **Files:** Copy to current workspace, commit to new repo
2. **Context:** Add to `docs/history/CHANGELOG.md` or `docs/history/DECISIONS.md`
3. **Commits:** Extract commit messages, add to `docs/history/COMMIT_HISTORY.md`

**Don't:** Try to merge git histories (too complex, risk of mess)

---

## Phase 4: Final Polish

### Step 4.1: Repository Structure

Ensure clean structure:
```
accord-engine/
├── README.md                    # Main entry point
├── LICENSE                      # License file
├── .gitignore                   # Comprehensive ignore rules
├── docker-compose.yml           # Dev environment
├── docker-compose.prod.yml      # Production environment
├── scripts/                     # Utility scripts
├── docs/                        # Documentation
│   ├── DEPLOYMENT.md
│   ├── SECURITY_AND_COMPLIANCE.md
│   ├── WORKFLOWS.md
│   ├── EXECUTION_STRATEGIES.md
│   └── history/                 # Archived internal docs
├── sales/                       # Sales materials
├── 01-Core-Execution-Engine/
├── 02-Safety-Observability/
├── 03-Intelligence-Memory-Service/
├── 04-Control-Plane-Orchestrator/
└── 05-Deploy-Monitoring-Infra/
```

### Step 4.2: README.md Update

Ensure main README is professional and buyer-facing.

### Step 4.3: Add Repository Description

On GitHub, add:
- **Description:** "Policy-driven web automation platform with compliance controls"
- **Topics:** automation, web-scraping, compliance, policy-driven, enterprise
- **Website:** (if you have one)

---

## Safe Execution Steps

### Step-by-Step (Do This)

**1. Clean Up Workspace (Safe - No Git Changes)**
```powershell
# I'll create a cleanup script you can review
# This will organize files, not touch git
```

**2. Review Cleanup**
- Review what will be archived
- Review what will be kept
- Adjust as needed

**3. Create New GitHub Repo**
- Go to GitHub, create new repo
- Don't initialize with anything

**4. Initialize Clean Git**
```powershell
# Remove old git connection (keeps local .git)
git remote remove origin

# Or start fresh (removes .git entirely)
# Backup .git first if you want: Copy .git folder somewhere
Remove-Item -Recurse -Force .git
git init
```

**5. Make Initial Commit**
```powershell
git add .
git commit -m "Initial commit: Production-ready Accord Engine

- Policy-driven web automation platform
- Production-ready core executors (Vanilla, Stealth, Assault)
- Enterprise advanced features (Ultimate Stealth, Custom)
- Complete compliance controls and audit logging
- Three production workflows (Page Change Detection, Job Posting Monitor, Uptime Smoke Check)
- Full test coverage and CI/CD
- One-command deployment (docker-compose)
- Production proof pack validation"
```

**6. Connect to New Repo**
```powershell
git remote add origin <YOUR_NEW_REPO_URL>
git branch -M main
git push -u origin main
```

**7. Verify**
- Check GitHub repo looks clean
- Verify all files are there
- Test clone in fresh directory

---

## What Gets Archived vs Kept

### Keep (User/Buyer-Facing)
- ✅ `README.md`
- ✅ `docs/reports/PRODUCTION_READINESS_REPORT.md`
- ✅ `docs/` (all user-facing docs)
- ✅ `sales/` (all sales materials)
- ✅ All source code
- ✅ All tests
- ✅ Docker files
- ✅ Scripts

### Archive to `docs/history/` (Internal Tracking)
- `CHIEF_ENGINEER_*.md` files
- `*_COMPLETE.md` files (implementation summaries)
- `*_STATUS.md` files
- `*_SUMMARY.md` files (unless valuable)

### Delete (Temporary/Redundant)
- Duplicate files
- Old backup files
- Temporary test files

---

## Safety Checklist

Before pushing:
- [ ] `.gitignore` is comprehensive
- [ ] No secrets in code (API keys, passwords)
- [ ] No large binary files
- [ ] Documentation is clean and professional
- [ ] README is buyer-facing
- [ ] All "COMPLETE" files reviewed (archive or delete)
- [ ] Test that repo can be cloned fresh
- [ ] Verify proof pack can run on fresh clone

---

## After Push

1. **Keep Old Repo:** Don't delete it yet
2. **Review Old Repo:** Look for valuable history
3. **Extract Value:** Manually copy valuable items to new repo
4. **Archive Old Repo:** Once satisfied, archive or delete old repo

---

## Need Help?

I can:
1. Create cleanup script (organizes files, doesn't touch git)
2. Create final README.md (professional, buyer-facing)
3. Review what to keep vs archive
4. Guide you through git commands step-by-step

**Just tell me what you want me to do first!**

---

**Last Updated:** 2024-01-01

