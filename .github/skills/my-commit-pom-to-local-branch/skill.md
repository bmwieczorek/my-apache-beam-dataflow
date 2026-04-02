---
name: my-commit-pom-to-local-branch
description: 'Commit pom.xml changes to a local Git branch. Use when: committing upgrade changes after all tests pass.'
---

# Commit to Local Branch

Commits the pom.xml changes to a new local Git branch after all upgrade steps have passed.

## Instructions

You are a Git commit assistant. Follow these steps to commit the upgrade changes:

### Step 1: Ask for user confirmation

Prompt the user:

```
Do you want to commit the pom.xml changes locally? (Y/n)
```

Default is **Yes** — if the user responds with anything other than `n` or `no`, proceed with the commit.

If the user declines, report SKIPPED and stop.

### Step 2: Determine the new Beam version

Read the `<beam.version>` from `pom.xml` to construct the branch name.

The branch name format is: `beam-upgrade-<new-beam-version>` (e.g. `beam-upgrade-2.72.0`).

### Step 3: Create branch and commit

**CRITICAL:** Only commit `pom.xml` — do NOT use `git add .` or `git add -A`. Other files in the working tree (skills, agents, readmes, etc.) must NOT be included in this commit.

```bash
git checkout -b beam-upgrade-<version>
git add pom.xml
git commit -m "Upgrade Apache Beam to <version>"
```

### Step 4: Report result

**Success criteria:**
- ✅ Branch created: `beam-upgrade-<version>`
- ✅ pom.xml committed with upgrade message

**If it fails:**
- ❌ Show the git error
- ❌ Suggest fixes (e.g. branch already exists, uncommitted changes)
