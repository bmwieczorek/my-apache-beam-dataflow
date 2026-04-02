---
name: my-upgrade-apache-beam-maven-dependencies
description: 'Upgrade Apache Beam and related Google Cloud dependencies following the BOM chain. Use when: upgrading Beam version, updating Google Cloud dependencies, checking for newer dependency versions.'
---

# Upgrade Apache Beam Maven Dependencies

Upgrades Apache Beam and related dependencies following the BOM (Bill of Materials) chain from Beam to Google Cloud libraries.

## Instructions

You are a Maven dependency upgrade assistant. Follow these steps to upgrade Apache Beam and related dependencies.

**BOM chain:** Beam → `libraries-bom` → `google-cloud-bom` → individual library versions (bigquery, storage, etc.)

### Step 1: Check current versions in pom.xml

Read `pom.xml` and note the current `<beam.version>` and all other dependency version properties.

### Step 2: Find latest Beam release

```bash
curl -s "https://repo1.maven.org/maven2/org/apache/beam/beam-sdks-java-core/maven-metadata.xml" | grep '<latest>' | sed 's/.*<latest>\(.*\)<\/latest>.*/\1/'
```

This returns the latest Beam version (e.g. `2.72.0`).

### Step 3: Get libraries-bom version from Beam

Use the Beam **minor** version for the branch name (e.g. `release-2.72`, NOT `release-2.72.0`):

```bash
BEAM_MINOR="2.72"  # extract from latest version, drop .0 patch
curl -s "https://raw.githubusercontent.com/apache/beam/release-${BEAM_MINOR}/buildSrc/src/main/groovy/org/apache/beam/gradle/BeamModulePlugin.groovy" | grep "libraries-bom"
```

This returns a line like: `google_cloud_platform_libraries_bom : "com.google.cloud:libraries-bom:26.76.0"` — extract the version number.

### Step 4: Resolve BigQuery and Storage versions via BOM chain

**Important:** The `libraries-bom` POM does NOT contain bigquery/storage directly. It imports `google-cloud-bom`, which has the actual versions. You must follow the chain:

**Step 4a:** Get `google-cloud-bom` version from `libraries-bom`:

```bash
LIBS_BOM="26.76.0"  # from Step 3
curl -s "https://repo1.maven.org/maven2/com/google/cloud/libraries-bom/${LIBS_BOM}/libraries-bom-${LIBS_BOM}.pom" | grep 'google-cloud-bom' -A1
```

This returns the `google-cloud-bom` version (e.g. `0.257.0`).

**Step 4b:** Get bigquery and storage versions from `google-cloud-bom`:

```bash
GC_BOM="0.257.0"  # from Step 4a
curl -s "https://repo1.maven.org/maven2/com/google/cloud/google-cloud-bom/${GC_BOM}/google-cloud-bom-${GC_BOM}.pom" | grep -E 'google-cloud-bigquery<|google-cloud-storage-bom' -A1
```

This returns both `google-cloud-bigquery` version and `google-cloud-storage-bom` version.

**Do NOT use** `maven-metadata.xml` for bigquery/storage — that returns the latest published version which may differ from the BOM-managed version.

### Step 5: Check other dependency versions (independently versioned)

All these curls return results. The `<latest>` tag may include alpha/beta versions, so also check stable versions when needed:

```bash
echo "=== hadoop ===" && curl -s "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/maven-metadata.xml" | grep '<latest>'
echo "=== parquet ===" && curl -s "https://repo1.maven.org/maven2/org/apache/parquet/parquet-avro/maven-metadata.xml" | grep '<latest>'
echo "=== slf4j ===" && curl -s "https://repo1.maven.org/maven2/org/slf4j/slf4j-api/maven-metadata.xml" | grep '<version>' | grep -v alpha | tail -1
echo "=== junit ===" && curl -s "https://repo1.maven.org/maven2/junit/junit/maven-metadata.xml" | grep '<latest>'
echo "=== commons-codec ===" && curl -s "https://repo1.maven.org/maven2/commons-codec/commons-codec/maven-metadata.xml" | grep '<latest>'
```

**Note:** For slf4j, `<latest>` returns alpha versions — use the `grep -v alpha | tail -1` pattern to get the latest stable version.

For major version bumps (e.g. parquet 1.15→1.17), keep the current version for safety unless explicitly requested.

### Step 6: Update pom.xml properties

Update version properties and the `libraries-bom` comment in `pom.xml`:
- `<beam.version>`
- `<google-cloud-bigquery.version>` (from Step 4b)
- `<google-cloud-storage.version>` (from Step 4b)
- `<hadoop.version>`, `<slf4j.version>`, `<commons-codec.version>` (from Step 5)
- Update the comment referencing `release-X.XX` and `libraries-bom` version

### Step 7: Present a summary table

| Dependency | Old Version | New Version | Source |
|---|---|---|---|
| beam | 2.71.0 | 2.72.0 | Maven Central |
| libraries-bom | 26.73.0 | 26.76.0 | Beam BeamModulePlugin.groovy |
| google-cloud-bom | — | 0.257.0 | libraries-bom POM |
| google-cloud-bigquery | 2.57.1 | 2.59.0 | google-cloud-bom |
| google-cloud-storage | 2.61.0 | 2.63.0 | google-cloud-bom |
| hadoop | 3.4.2 | 3.4.3 | Maven Central |
| slf4j | 2.0.16 | 2.0.17 | Maven Central (stable) |
| commons-codec | 1.17.1 | 1.21.0 | Maven Central |
