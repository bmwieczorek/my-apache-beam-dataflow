---
name: my-run-mvn-integration-tests
description: 'Run Maven integration tests with the itest profile. Use when: running integration tests, testing external services, end-to-end testing.'
---

# Run Maven Integration Tests

Runs integration tests using the `itest` Maven profile. These tests typically interact with external services, databases, or perform end-to-end testing.

## Instructions

You are a Maven integration test execution assistant. Follow these steps to run integration tests:

### Step 0: Output logging

**Important:** Integration tests can run for up to 1 hour each. You MUST set the terminal timeout to **3600000** (1 hour) to avoid premature timeout. Do NOT use shorter timeouts.

**CRITICAL — surefire reports are unreliable:** The integration tests internally run `mvn clean package` as a subprocess to rebuild the uber jar. This deletes `target/surefire-reports/`, destroying any previously written reports. Only the LAST test class's surefire report survives. Therefore, **do NOT rely on surefire reports** for per-test results.

**Solution:** Pipe output through `tee` to capture the full log to `/tmp/mvn-itest-output.log`, then grep that file for per-test results:

```bash
mvn test -Pitest 2>&1 | tee /tmp/mvn-itest-output.log
```

After the test completes, run this **separate terminal command** to extract per-test-class results:

```bash
grep 'Tests run:.*-- in' /tmp/mvn-itest-output.log
```

This separate command is **mandatory** — do NOT rely on the truncated terminal output or surefire reports.

**Always** present a summary table of all test classes (built from the grep output):

| Test Class | Tests | Failures | Errors | Skipped | Duration |
|---|---|---|---|---|---|
| SomeIntegrationTest | 3 | 0 | 0 | 0 | 5.234 s |
| **Total** | **3** | **0** | **0** | **0** | **5.234s** |

### Step 1: Check itest profile configuration

Verify in pom.xml:
- Profile ID: `itest`
- Surefire is bound to the `test` phase
- Surefire configuration: Includes `**/itest/*IntegrationTest`

### Step 2: Run integration tests

First run clean separately, then run tests with output captured via `tee`:

```bash
mvn clean
mvn test -Pitest 2>&1 | tee /tmp/mvn-itest-output.log
```

**After the `mvn test` command completes**, you MUST run this as a **separate terminal command** to get per-test-class results:

```bash
echo "=== PER-TEST RESULTS ===" && grep 'Tests run:.*-- in' /tmp/mvn-itest-output.log && echo "=== TOTALS ===" && grep '^\[INFO\] Tests run:' /tmp/mvn-itest-output.log | tail -1
```

Use this grep output (not the terminal output or surefire reports) to build the summary table.

**Why `tee /tmp/...`?** Two reasons:
1. Terminal output gets truncated at ~60KB, losing per-test lines
2. Integration tests internally run `mvn clean package` which deletes `target/surefire-reports/`, so only the last test's surefire report survives — grepping surefire reports won't show all tests

**Note:** The `itest` profile binds surefire's `test` goal to the `test` phase, running only `*IntegrationTest.java` files in the `itest` subdirectory.

**Expected behavior:**
- Compiles main and test sources
- Runs `*IntegrationTest.java` files from the `itest` package subdirectory
- Each integration test can run for up to 1 hour (they deploy GCP infrastructure via Terraform, run Dataflow pipelines, and tear down resources)
- May require environment setup (GCP credentials, Terraform, etc.)

### Step 3: Monitor test execution

Watch for:
- Integration test class names
- External service connections
- Longer execution times
- Resource setup/teardown

**Example output:**
```
[INFO] Running com.bawi.beam.dataflow.SomeIntegrationTest
[INFO] Tests run: 3, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 5.234 s
[INFO] Running com.bawi.beam.dataflow.DataflowIT
[INFO] Tests run: 2, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 12.456 s
...
[INFO] Results:
[INFO] Tests run: 15, Failures: 0, Errors: 0, Skipped: 2
[INFO] BUILD SUCCESS
```

### Step 4: Analyze results

**Success criteria:**
- ✅ `BUILD SUCCESS`
- ✅ `Failures: 0`
- ✅ `Errors: 0`
- ✅ All external connections successful

**Common issues:**
- ❌ Connection timeouts (external services unavailable)
- ❌ Authentication failures (credentials not configured)
