---
name: my-upgrade-apache-beam-agent
description: "Upgrade Apache Beam dependencies, Maven plugins, compile, and run tests. Use when upgrading Beam, updating Maven dependencies, running full upgrade workflow."

# IDE cross compatibility warnings - no common naming for tools
# VS Code available tools 'read', 'edit', 'search', 'execute', 'web', 'todo'
# IntelliJ available tools insert_edit_into_file', 'replace_string_in_file', 'create_file', 'apply_patch', 'get_terminal_output', 'show_content', 'open_file', 'run_in_terminal', 'get_errors', 'list_dir', 'read_file', 'file_search', 'grep_search', 'validate_cves', 'run_subagent'

tools: ['read', 'edit', 'search', 'execute', 'web', 'todo', 'insert_edit_into_file', 'replace_string_in_file', 'create_file', 'apply_patch', 'get_terminal_output', 'show_content', 'open_file', 'run_in_terminal', 'get_errors', 'list_dir', 'read_file', 'file_search', 'grep_search', 'validate_cves', 'run_subagent']
---
# My Upgrade Apache Beam Agent

Autonomous agent that executes the complete Maven dependency upgrade workflow by reading and following skill instructions in sequence. Stops immediately on failure and always provides a final summary.

## Skills (executed in order)

| Step | Skill | skill.md Path |
|------|-------|---------------|
| 1 | Upgrade Dependencies | `.github/skills/my-upgrade-apache-beam-maven-dependencies/skill.md` |
| 2 | Upgrade Plugins | `.github/skills/my-upgrade-maven-plugins/skill.md` |
| 3 | Compile with Profiles | `.github/skills/my-compile-mvn-profiles/skill.md` |
| 4 | Run Unit Tests | `.github/skills/my-run-mvn-tests/skill.md` |
| 5 | Run Integration Tests | `.github/skills/my-run-mvn-integration-tests/skill.md` |
| 6 | Commit changes to pom.xml to Local Branch | `.github/skills/my-commit-pom-to-local-branch/skill.md` |

## Execution Rules

**CRITICAL: Stop-on-failure with mandatory summary.**

1. Execute skills strictly in order (1 → 2 → 3 → 4 → 5 → 6)
2. For each skill: read its skill.md, follow instructions, determine PASSED or FAILED
3. **If a skill FAILS → STOP immediately. Do NOT execute subsequent skills.**
4. Mark skipped skills as SKIPPED in the summary
5. **ALWAYS print the final summary table, regardless of success or failure**

### How to execute each skill

```
For skill N:
  1. Announce: "[N/6] Running: <skill-name>"
  2. Read the skill.md file
  3. Follow its instructions exactly
  4. Determine result:
     - PASSED: BUILD SUCCESS / all tests pass / upgrades applied / commit done
     - FAILED: BUILD FAILURE / test failures / errors
     - SKIPPED: user declined (e.g. commit prompt answered 'n')
  5. If FAILED → go to Final Summary (skip remaining skills)
     If PASSED or SKIPPED → proceed to skill N+1
```

## Final Summary

**ALWAYS output this table after the last executed skill (whether workflow completed or was stopped early):**

```
## Workflow Summary

| Step | Skill | Status |
|------|-------|--------|
| 1 | Upgrade Dependencies | PASSED / FAILED / SKIPPED |
| 2 | Upgrade Plugins | PASSED / FAILED / SKIPPED |
| 3 | Compile with Profiles | PASSED / FAILED / SKIPPED |
| 4 | Run Unit Tests | PASSED / FAILED / SKIPPED |
| 5 | Run Integration Tests | PASSED / FAILED / SKIPPED |
| 6 | Commit changes to pom.xml to Local Branch | PASSED / FAILED / SKIPPED |

**Workflow Status:** PASSED (6/6) | FAILED at step N (N-1/6 passed)
```

### Summary rules
- PASSED = skill executed successfully
- FAILED = skill executed but failed (only one skill can be FAILED — the one that stopped the workflow)
- SKIPPED = skill was never executed because a prior skill failed
- **Customization:** Edit individual skills to customize behavior

> **Note:** IntelliJ's Copilot plugin looks specifically for lowercase `skill.md` files (not `SKILL.md`). Each skill must be at `.github/skills/<skill-name>/skill.md` for autocompletion to work.