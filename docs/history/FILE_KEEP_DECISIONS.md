# File Keep/Archive Decisions

**Clear recommendations for each file in root directory**

---

## ✅ KEEP (Production/Buyer-Facing)

### Essential Files
- ✅ **README.md** - Main entry point, buyer-facing
- ✅ **docs/reports/PRODUCTION_READINESS_REPORT.md** - Buyer-facing readiness report
- ✅ **LICENSE** - Required legal file
- ✅ **.gitignore** - Required for git
- ✅ **docker-compose.yml** - Development deployment
- ✅ **docker-compose.prod.yml** - Production deployment

**Why:** These are essential for the repository and buyer evaluation.

---

## 📦 ARCHIVE to `docs/history/` (Internal Tracking)

### Chief Engineer Files (Internal Process)
- 📦 **CHIEF_ENGINEER_ACTION_PLAN.md** - Internal planning doc
- 📦 **CHIEF_ENGINEER_AUDIT.md** - Internal audit
- 📦 **CHIEF_ENGINEER_COMPLETION.md** - Internal completion tracking
- 📦 **CHIEF_ENGINEER_FINAL_REPORT.md** - Internal final report
- 📦 **CHIEF_ENGINEER_STATUS.md** - Internal status tracking

**Why:** Internal tracking documents. Not buyer-facing, but might have historical value.

### Implementation Summaries (Internal Tracking)
- 📦 **COMMERCIAL_PACKAGING_COMPLETE.md** - Implementation summary
- 📦 **COMPLIANCE_AND_RISK_CONTROLS_COMPLETE.md** - Implementation summary
- 📦 **ENTERPRISE_ADVANCED_FEATURES_COMPLETE.md** - Implementation summary
- 📦 **SALES_KIT_COMPLETE.md** - Implementation summary
- 📦 **WORKFLOWS_COMPLETE.md** - Implementation summary
- 📦 **WORKFLOWS_IMPLEMENTATION_SUMMARY.md** - Implementation summary

**Why:** Internal tracking of what was completed. Not needed for buyers, but good historical record.

### Validation/Status Files (Internal Tracking)
- 📦 **STATUS_UPDATE_INVESTIGATION.md** - Internal investigation
- 📦 **VALIDATION_PHASES.md** - Internal validation tracking
- 📦 **PROOF_PACK_SUMMARY.md** - Internal summary (we have full PROOF_PACK.md)
- 📦 **VALUATION_READY_SUMMARY.md** - Internal summary

**Why:** Internal tracking. The actual proof pack and production report are what buyers need.

### Other Internal Docs
- 📦 **HUMAN_VALIDATION_GUIDE.md** - Internal validation guide
- 📦 **SECURITY_ARCHITECTURE.md** - Internal architecture doc (we have SECURITY_AND_COMPLIANCE.md)

**Why:** Internal docs. We have buyer-facing versions in `docs/`.

---

## ⚠️ DECISION NEEDED (Migration Helpers)

### Migration Files (Temporary - Delete After Migration)
- ⚠️ **CLEANUP_AND_MIGRATION_PLAN.md** - Helpful for migration, delete after?
- ⚠️ **MIGRATION_CHECKLIST.md** - Helpful for migration, delete after?
- ⚠️ **FINAL_CLEANUP_SUMMARY.md** - Helpful for migration, delete after?

**Recommendation:** 
- **Option A:** Keep in `docs/history/` - Shows migration was done professionally
- **Option B:** Delete after migration - Cleaner, but loses context

**My Recommendation:** Archive to `docs/history/` - Shows professional migration process.

---

## Summary

### Keep in Root (6 files)
- README.md
- docs/reports/PRODUCTION_READINESS_REPORT.md
- LICENSE
- .gitignore
- docker-compose.yml
- docker-compose.prod.yml

### Archive to `docs/history/` (18 files)
- All CHIEF_ENGINEER_*.md files (5)
- All *_COMPLETE.md files (5)
- All *_SUMMARY.md files (3)
- Internal tracking files (5)

### Migration Helpers (3 files)
- Archive to `docs/history/` after migration (recommended)
- Or delete after migration (cleaner)

---

## Final Recommendation

**Keep:** 6 essential files in root  
**Archive:** 18 internal tracking files to `docs/history/`  
**Migration Helpers:** Archive to `docs/history/` (shows professional process)

**Result:**
- Clean, professional root directory
- All internal tracking preserved in history folder
- Buyers see only production-ready files
- Historical context preserved for future reference

---

**Ready to proceed?** The cleanup script will handle this automatically.

