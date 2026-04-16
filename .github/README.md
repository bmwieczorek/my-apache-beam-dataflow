# Skills & Agent — Readme

## Project Structure

```
.github/
├── agents/
│   └── my-upgrade-apache-beam-agent.agent.md    # orchestrator agent
└── skills/
    ├── my-upgrade-apache-beam-maven-dependencies/
    │   └── skill.md                              # step 1: upgrade Beam + BOM chain deps
    ├── my-upgrade-maven-plugins/
    │   └── skill.md                              # step 2: upgrade Maven plugins
    ├── my-compile-mvn-profiles/
    │   └── skill.md                              # step 3: compile dist + direct-runner
    ├── my-run-mvn-tests/
    │   └── skill.md                              # step 4: run unit tests
    ├── my-run-mvn-integration-tests/
    │   └── skill.md                              # step 5: run integration tests
    └── my-commit-pom-to-local-branch/
        └── skill.md                              # step 6: git commit pom.xml
```

- Skills and agents live directly under `.github/` (not `.github/copilot/`).
- Each skill is a folder containing a single `skill.md` file.
- The agent file is `<name>.agent.md` in `.github/agents/`.

## Naming Conventions

| Element | Pattern | Example |
|---------|---------|---------|
| Skill folder | `my-<verb>-<noun>/` | `my-run-mvn-tests/` |
| Skill file | `skill.md` (lowercase) | `.github/skills/my-run-mvn-tests/skill.md` |
| Agent file | `my-<name>.agent.md` | `my-upgrade-apache-beam-agent.agent.md` |
| Prefix | `my-` for all custom skills and agents | distinguishes from built-in ones |

## Architecture

### Dual Invocation — Independent Skills + Agent Orchestration

Skills can be used in two ways:

1. **Independently** — invoke a single skill directly (e.g. just compile, just run tests)
2. **Via agent** — the orchestrator agent reads each `skill.md` in sequence and follows its instructions

```
┌──────────────────────────────────────────────────────────────┐
│                    Direct Invocation                         │
│   User ──► /my-run-mvn-tests ──► skill.md ──► executes      │
└──────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────┐
│                  Agent Orchestration                          │
│   User ──► @my-upgrade-apache-beam-agent                     │
│              ├─► step 1: reads skill.md ──► upgrade deps     │
│              ├─► step 2: reads skill.md ──► upgrade plugins  │
│              ├─► step 3: reads skill.md ──► compile          │
│              ├─► step 4: reads skill.md ──► unit tests       │
│              ├─► step 5: reads skill.md ──► integration tests│
│              └─► step 6: reads skill.md ──► git commit       │
└──────────────────────────────────────────────────────────────┘
```

The agent stops on first failure and always prints a summary table with PASSED / FAILED / SKIPPED per step.

### Skill Format

Each `skill.md` has YAML frontmatter + markdown instructions:

```yaml
---
name: my-run-mvn-tests
description: 'Run Maven unit tests. Use when: running unit tests, verifying test results.'
---

# Title

## Instructions
...step-by-step instructions...
```

## IDE Compatibility — VS Code and IntelliJ

| Feature | VS Code | IntelliJ (JetBrains 2026.1+) |
|---------|---------|------------------------------|
| Skills location | `.github/skills/<name>/skill.md` | `.github/skills/<name>/skill.md` |
| Agents location | `.github/agents/<name>.agent.md` | `.github/agents/<name>.agent.md` |
| Invoke skill | type `/my-skill-name` in chat | type `/my-skill-name` in chat |
| Invoke agent | type `@my-agent-name` in chat | type `@my-agent-name` in chat |
| File casing | `skill.md` (lowercase required for IntelliJ autocompletion) | `skill.md` (lowercase required) |

**Key:** Use lowercase `skill.md` — IntelliJ's Copilot plugin requires it for autocompletion. VS Code works with either case but lowercase keeps it portable.

## Lessons Learned

### Terminal output truncation
Maven output exceeds 60KB and gets truncated, losing per-test-class results. Solutions:
- **Unit tests:** Run `mvn clean test`, then grep `target/surefire-reports/*.txt` in a separate command (surefire reports are reliable for unit tests)
- **Integration tests:** Use `mvn test -Pitest 2>&1 | tee /tmp/mvn-itest-output.log`, then grep the tee'd file (surefire reports are NOT reliable — integration tests internally run `mvn clean package` which destroys them)

### Surefire reports destroyed by integration tests
Integration tests (`-Pitest`) internally invoke `mvn clean package`, which deletes `target/surefire-reports/`. Only the last test class's report survives. Fix: use `tee` to capture full output to a file.

### Maven profile behavior
The `dist` profile has `activeByDefault=true`, so `-Pdist` is redundant for unit tests. Plain `mvn clean test` activates it automatically.

### Compile with clean per profile
Run `mvn clean compile -P<profile>` for each profile separately to avoid stale classes from a prior profile lingering in `target/`.

### BOM chain for dependency versions
Beam → `libraries-bom` → `google-cloud-bom` → individual library versions (bigquery, storage). Do NOT use `maven-metadata.xml` for BOM-managed deps — it returns the latest published version, not the BOM-managed one. Follow the chain through POM files instead.

### Slf4j version resolution
The `<latest>` tag in Maven metadata can return alpha versions. Use `grep -v alpha | tail -1` to get the latest stable.

### Git commit scope
Only `git add pom.xml` — never `git add .`. Other working tree files (skills, agents, docs) must not be included in the upgrade commit.

### Skill file location
Skills must be at `.github/skills/<name>/skill.md` — not nested deeper. Both VS Code and IntelliJ look for this exact structure.

## Running from Copilot CLI

Use the Copilot CLI to run the agent in fully autonomous mode with a prompt file:

```bash
copilot --agent=my-upgrade-apache-beam-agent \
  --autopilot \
  --allow-tool='shell(mvn:*)' \
  --allow-tool='shell(git:*)' \
  --allow-tool='shell(curl)' \
  --allow-tool='shell(grep)' \
  --allow-tool='shell(sed)' \
  --allow-tool='shell(awk)' \
  --allow-tool='shell(tail)' \
  --allow-tool='shell(head)' \
  --allow-tool='shell(echo)' \
  --allow-tool='shell(tee)' \
  --allow-tool='shell(cat)' \
  --allow-tool='shell(find)' \
  --allow-tool='write' \
  --allow-url=repo1.maven.org \
  --allow-url=raw.githubusercontent.com \
  --max-autopilot-continues 100 \
  --model=claude-sonnet-4.6 \
  --effort=high \
  --add-dir ~/.copilot \
  --add-dir ~/dev/my-apache-beam-dataflow \
  --add-dir /tmp \
  -p "$(cat .github/prompts/my-upgrade-beam-no-itests-rca-fix-flaky-tests.prompt.md)"
```

### CLI flags explained

| Flag | Purpose |
|------|---------|
| `--agent` | Selects the orchestrator agent |
| `--autopilot` | Non-interactive — no user confirmations |
| `--allow-tool='shell(cmd:*)'` | Pre-approve shell commands (required in autopilot) |
| `--allow-tool='write'` | Allow file writes |
| `--allow-url` | Allow HTTP fetches to Maven Central and GitHub raw content |
| `--max-autopilot-continues` | Max autonomous steps before stopping |
| `--model` | LLM model to use |
| `--effort` | Reasoning effort level (`low`, `medium`, `high`) |
| `--add-dir` | Additional directories the agent can access |
| `-p` | Prompt text (use `$(cat ...)` to load from file) |

### Model options

| Model | Best for |
|-------|----------|
| `claude-sonnet-4.6` + `--effort=high` | Fast, cost-effective — good default for upgrades |
| `claude-opus-4.6` | More capable reasoning — use for complex flaky test fixes |

### Notes

- The `-p` flag requires a literal string. The `/prompt` syntax only works inside the IDE chat, not in the CLI. Use `$(cat .github/prompts/<name>.prompt.md)` to load a prompt file.
- Every shell command the agent might run must be in `--allow-tool`. If you see `Permission denied and could not request permission from user`, add the missing command (e.g. `--allow-tool='shell(xargs:*)'`).

