---
name: my-run-mvn-tests
description: 'Run Maven unit tests with the default (dist) profile using Surefire plugin. Use when: running unit tests, verifying test results, checking test failures.'
---

# Run Maven Unit Tests

Runs all unit tests using Maven Surefire plugin. Integration tests (matching `**/*IntegrationTest`) are excluded by default.

## Instructions

You are a Maven test execution assistant. Follow these steps to run unit tests:

### Step 0: Output logging

Run `mvn test` directly in the terminal (do NOT pipe through `tail` or redirect output).

**CRITICAL:** Terminal output can be very large (60KB+) and gets truncated, losing per-test-class result lines. Do NOT rely on the mvn terminal output for per-test results. Instead, after the test completes, run a **separate terminal command** to collect per-test results from surefire reports:

```bash
grep -h 'Tests run:' target/surefire-reports/*.txt
```

This separate command is **mandatory** — it is the only reliable way to get all per-test-class results.

**Note:** Unlike integration tests, unit tests do NOT run `mvn clean` internally, so `target/surefire-reports/` is intact and reliable.

**Always** present a summary table of all test classes (built from the surefire grep output):

| Test Class | Tests | Failures | Errors | Skipped | Duration |
|---|---|---|---|---|---|
| SomeTest | 5 | 0 | 0 | 0 | 0.123 s |
| AnotherTest | 3 | 0 | 0 | 0 | 0.045 s |
| ... | ... | ... | ... | ... | ... |
| **Total** | **107** | **0** | **0** | **5** | **1m 56s** |

### Step 1: Identify test configuration

Check pom.xml for surefire configuration:
- Default: Excludes `**/*IntegrationTest`
- Includes: `**/*Test.java`
- The `dist` profile is `activeByDefault=true`, so no need to pass `-Pdist` explicitly

### Step 2: Run unit tests

Run clean first to remove stale surefire reports from prior runs (e.g. integration tests leave their reports in `target/surefire-reports/`):

```bash
mvn clean test
```

**After the `mvn test` command completes**, you MUST run this as a **separate terminal command** to get per-test-class results (since mvn output is often truncated):

```bash
echo "=== PER-TEST RESULTS ===" && grep -h 'Tests run:' target/surefire-reports/*.txt && echo "=== TOTALS ===" && grep -h 'Tests run:' target/surefire-reports/*.txt | awk -F'[,:]+' '{t+=$2; f+=$4; e+=$6; s+=$8} END {printf "Tests run: %d, Failures: %d, Errors: %d, Skipped: %d\n", t, f, e, s}'
```

Use this grep output (not the mvn terminal output) to build the summary table.

### Step 3: Monitor test execution

Watch for:
- Test class names being executed
- Individual test method results
- Any failures or errors
- Test execution time

**Example output:**
```
[INFO] Running com.bawi.beam.dataflow.SomeTest
[INFO] Tests run: 5, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.123 s
[INFO] Running com.bawi.beam.dataflow.AnotherTest
[INFO] Tests run: 3, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.045 s
...
[INFO] Results:
[INFO] Tests run: 107, Failures: 0, Errors: 0, Skipped: 5
[INFO] BUILD SUCCESS
```

### Step 4: Analyze results

**Success criteria:**
- ✅ `BUILD SUCCESS`
- ✅ `Failures: 0`
- ✅ `Errors: 0`
- ✅ Skipped tests are expected (integration tests excluded)

**If failures occur:**
- ❌ Show which test class/method failed
- ❌ Show failure reason
- ❌ Suggest investigation steps
