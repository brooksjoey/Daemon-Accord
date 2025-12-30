# Development Workflow - Daemon Accord

**Strategy:** Simple, direct development on `main` branch

---

## Branch Strategy

### `main` Branch
- **Purpose:** Production-ready code
- **Status:** Always stable, tested, deployable
- **Use:** All development happens here
- **Rule:** Test before committing, keep it deployable

---

## Workflow

### Daily Development
```bash
# Make sure you're on main
git checkout main

# Pull latest changes
git pull origin main

# Make your changes, then commit
git add .
git commit -m "Add: your feature description"

# Push to GitHub
git push origin main
```

### For Larger Features (Optional)
```bash
# Create a feature branch
git checkout -b feature/your-feature-name

# ... work on feature ...

# When done, merge back to main
git checkout main
git merge feature/your-feature-name
git push origin main

# Delete the feature branch
git branch -d feature/your-feature-name
```

---

## Quick Commands

### Check Current Branch
```bash
git branch
```

### See Recent Commits
```bash
git log --oneline -10
```

### Undo Last Commit (Keep Changes)
```bash
git reset --soft HEAD~1
```

---

## Rules

1. **Test before committing** - Keep main stable
2. **Keep main deployable** - Should always work
3. **Document features** - Update README/docs when adding features
4. **Tag releases** - Tag stable versions (optional)
5. **Write clear commit messages** - Helps track changes

---

## Current Status

- ✅ `main` branch: Production-ready, all development happens here
- 📦 Pushed to GitHub

---

**Remember:** Keep it simple, test your changes, and maintain a working product!
