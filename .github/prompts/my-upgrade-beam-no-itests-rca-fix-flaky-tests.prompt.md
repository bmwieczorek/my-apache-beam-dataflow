---
description: "Upgrade Apache Beam dependencies (skip integration tests). Automatically analyzes and fixes flaky unit tests; provides detailed root cause analysis and suggested fixes for any that cannot be resolved automatically."
---
Run the full upgrade workflow autonomously in non-interactive mode
with these overrides and instructions:

## Workflow Overrides

- **Skip Step 5 (Integration Tests):** Do NOT run integration tests.
  Execute steps 1 → 2 → 3 → 4 → 6 (skip itest).

## Flaky Test Handling (Step 4 — Unit Tests)

If any unit tests fail:
1. Analyze the root cause in detail (not just the stack trace —
   explain WHY it fails: API change, behavioral change, timing
   issue, dependency conflict, etc.).
2. Determine how to reproduce the failure reliably.
3. Attempt to fix the code or test so it passes — do NOT
   exclude/skip the test.
4. Re-run unit tests to confirm the fix works.

If a test **cannot be fixed automatically**:
- Do NOT skip or `@Ignore` it.
- **STOP the workflow** — do NOT proceed to Step 6 (commit).
- In the final output, include a **detailed root cause analysis**
  for each unfixed test with ALL of the following:
  1. **Test class and method name**
  2. **Error message and relevant stack trace excerpt**
  3. **Root cause explanation** — why the test fails after the
     upgrade (e.g. "Beam 2.x changed the default coder for X",
     "API method Y was deprecated and removed").
  4. **Suggested fix** — concrete steps or code changes the user
     can apply manually to resolve the failure.
  5. **Upstream references** — links to relevant Beam JIRA issues,
     release notes, or migration guides if applicable.
- Then skip to the **Final Output** section and report the workflow
  as FAILED at Step 4.

## Upfront Decisions (no prompts)

1. **Commit (Step 6):** YES — commit ALL changes locally to the
   upgrade branch, including `pom.xml` version updates AND any
   flaky test fixes (e.g. modified test source files). Stage all
   changed files with `git add -A` and create a single commit with
   a message summarizing both the dependency upgrades and the test
   fixes (e.g. "Upgrade Apache Beam to 2.x.0 and fix flaky tests").
   **Only reached if ALL unit tests pass** (fixed or otherwise).
2. **Major version bumps:** SKIP — only apply minor/patch upgrades.
   Keep current version for any major version change.
3. **Alpha/Beta/RC versions:** SKIP — use latest stable only.
4. **Do NOT ask for any user confirmation at any step.** Proceed
   autonomously with these defaults throughout the entire workflow.

## Pre-Commit Summary (before Step 6)

After all tests pass and BEFORE executing the commit (Step 6), print
a complete summary of all changes to the screen:

1. **Dependency changes table** — for every dependency or plugin whose
   version changed in `pom.xml`, show:

   | Artifact | Old Version | New Version | Change Type |
   |----------|-------------|-------------|-------------|
   | example:artifact | 1.0.0 | 1.1.0 | minor |

2. **Flaky test fixes** — for each test that was fixed, show:
   - Test class and method name
   - Root cause (why it failed)
   - What was changed to fix it

3. **Unfixed test failures** — for each test that could NOT be fixed
   automatically, show:
   - Test class and method name
   - Error message and relevant stack trace excerpt
   - Root cause explanation
   - Suggested manual fix (concrete code changes or steps)
   - Upstream references (Beam JIRA, release notes, migration guides)

4. **Full `git diff`** — run `git diff` and print the complete output
   so all pending changes are visible before they are committed.

## Final Output

After the workflow completes (success or failure), print ALL of the
following to the screen:

1. **Workflow summary table** — all 6 steps with PASSED/FAILED/SKIPPED status.

2. **Dependency changes table** — for every dependency or plugin whose
   version changed, show:

   | Artifact | Old Version | New Version | Change Type |
   |----------|-------------|-------------|-------------|
   | org.apache.beam:beam-sdks-java-core | 2.x.0 | 2.y.0 | minor |

   Include Maven plugins in the same table.

3. **Code changes summary** — list every file modified and what was
   changed (e.g. "pom.xml: updated 12 dependency versions",
   "src/test/java/…Test.java: fixed assertion for new API").

4. **Unfixed test failures** — if any tests could NOT be fixed
   automatically, show for each:
   - Test class and method name
   - Error message and relevant stack trace excerpt
   - Root cause explanation (why it fails after the upgrade)
   - Suggested manual fix (concrete code changes or steps)
   - Upstream references (Beam JIRA, release notes, migration guides)

5. **Full `git diff`** — run `git diff` and print the complete output
   so the user can see exactly what changed in the codebase.
