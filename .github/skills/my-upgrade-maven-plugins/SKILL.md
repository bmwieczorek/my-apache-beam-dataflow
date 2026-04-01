---
name: my-upgrade-maven-plugins
description: 'Upgrade Maven plugins to their latest stable versions. Use when: upgrading build plugins, checking for newer plugin versions.'
---

# Upgrade Maven Plugins

Upgrades Maven plugins defined in `pom.xml` to their latest stable versions.

## Instructions

You are a Maven plugin upgrade assistant. Follow these steps to upgrade plugins:

### Step 1: Identify current plugin versions

Read `pom.xml` and collect all plugin version properties:
- `<maven-compiler-plugin.version>`
- `<maven-exec-plugin.version>`
- `<maven-shade-plugin.version>`
- `<maven-surefire-plugin.version>`
- `<jib-maven-plugin.version>`
- `<maven-enforcer-plugin.version>`

### Step 2: Check latest versions on Maven Central

For each plugin, fetch the latest version from Maven Central metadata:

```bash
curl -s "https://repo1.maven.org/maven2/org/apache/maven/plugins/maven-compiler-plugin/maven-metadata.xml" | grep '<latest>'
curl -s "https://repo1.maven.org/maven2/org/codehaus/mojo/exec-maven-plugin/maven-metadata.xml" | grep '<latest>'
curl -s "https://repo1.maven.org/maven2/org/apache/maven/plugins/maven-shade-plugin/maven-metadata.xml" | grep '<latest>'
curl -s "https://repo1.maven.org/maven2/org/apache/maven/plugins/maven-surefire-plugin/maven-metadata.xml" | grep '<latest>'
curl -s "https://repo1.maven.org/maven2/com/google/cloud/tools/jib-maven-plugin/maven-metadata.xml" | grep '<latest>'
curl -s "https://repo1.maven.org/maven2/org/apache/maven/plugins/maven-enforcer-plugin/maven-metadata.xml" | grep '<latest>'
```

### Step 3: Update pom.xml properties

Update each plugin version property to the latest stable version. Skip alpha/beta/RC versions — use only stable releases.

### Step 4: Present a summary table

| Plugin | Old Version | New Version |
|---|---|---|
| maven-compiler-plugin | 3.14.0 | 3.15.0 |
| exec-maven-plugin | 3.6.0 | 3.6.3 |
| ... | ... | ... |
