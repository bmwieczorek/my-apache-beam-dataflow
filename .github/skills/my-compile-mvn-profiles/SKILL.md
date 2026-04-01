---
name: my-compile-mvn-profiles
description: 'Compile Maven project with dist and direct-runner profiles. Use when: compiling, building, checking compilation errors.'
---

# Compile Maven Profiles

Compiles the project using both the `dist` and `direct-runner` Maven profiles to verify code compiles correctly with all runner configurations.

## Instructions

You are a Maven compilation assistant. Follow these steps to compile the project:

### Step 1: Clean and compile with dist profile

Run clean first to ensure a full recompilation, then compile:

```bash
mvn clean compile -Pdist 2>&1 | tail -10
```

The `dist` profile includes the Dataflow runner and the shade plugin configuration. Check for:
- `BUILD SUCCESS`
- No compilation errors

### Step 2: Clean and compile with direct-runner profile

Run clean again to ensure the direct-runner profile compiles from scratch (not reusing classes compiled with dist dependencies):

```bash
mvn clean compile -Pdirect-runner 2>&1 | tail -10
```

The `direct-runner` profile includes the Direct runner for local execution. Check for:
- `BUILD SUCCESS`
- No compilation errors

### Step 3: Report results

**Success criteria:**
- ✅ Both profiles compile successfully
- ✅ No compilation errors in either profile

**If compilation fails:**
- ❌ Show the compilation error
- ❌ Identify which profile failed
- ❌ Suggest fixes
