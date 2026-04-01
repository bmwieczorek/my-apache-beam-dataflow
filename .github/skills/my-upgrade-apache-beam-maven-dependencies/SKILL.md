---
name: my-upgrade-apache-beam-maven-dependencies
description: 'Upgrade Apache Beam and related Google Cloud dependencies following the BOM chain. Use when: upgrading Beam version, updating Google Cloud dependencies, checking for newer dependency versions.'
---

# Upgrade Apache Beam Maven Dependencies

Upgrades Apache Beam and related dependencies following the BOM (Bill of Materials) chain from Beam to Google Cloud libraries.

## Instructions

You are a Maven dependency upgrade assistant. Follow these steps to upgrade Apache Beam and related dependencies:

### Step 1: Check current Beam version

Read `pom.xml` and note the current `<beam.version>` property value.

### Step 2: Find latest Beam release

Fetch the latest stable Beam release version from Maven Central:

```bash
curl -s "https://repo1.maven.org/maven2/org/apache/beam/beam-sdks-java-core/maven-metadata.xml" | grep '<latest>' | sed 's/.*<latest>\(.*\)<\/latest>.*/\1/'
```

### Step 3: Determine the libraries-bom version used by Beam

Look up the Beam release's `BeamModulePlugin.groovy` to find the `google_cloud_platform_libraries_bom` version:

```bash
curl -s "https://raw.githubusercontent.com/apache/beam/release-{VERSION}/buildSrc/src/main/groovy/org/apache/beam/gradle/BeamModulePlugin.groovy" | grep "google_cloud_platform_libraries_bom"
```

Replace `{VERSION}` with the Beam version (e.g., `2.72`).

### Step 4: Resolve transitive dependency versions from libraries-bom

From the `libraries-bom` version, look up the managed versions for:
- `google-cloud-bigquery` — check at: `https://repo1.maven.org/maven2/com/google/cloud/google-cloud-bigquery/maven-metadata.xml`
- `google-cloud-storage` — check at: `https://repo1.maven.org/maven2/com/google/cloud/google-cloud-storage/maven-metadata.xml`

Cross-reference with the libraries-bom POM to find the exact versions it manages:

```bash
curl -s "https://repo1.maven.org/maven2/com/google/cloud/libraries-bom/{BOM_VERSION}/libraries-bom-{BOM_VERSION}.pom" | grep -A2 'google-cloud-bigquery\|google-cloud-storage'
```

### Step 5: Check other dependency versions

Check latest stable versions for independently versioned dependencies:

- **hadoop**: `curl -s "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/maven-metadata.xml" | grep '<latest>'`
- **parquet**: `curl -s "https://repo1.maven.org/maven2/org/apache/parquet/parquet-avro/maven-metadata.xml" | grep '<latest>'`
- **slf4j**: `curl -s "https://repo1.maven.org/maven2/org/slf4j/slf4j-api/maven-metadata.xml" | grep '<latest>'`
- **junit**: `curl -s "https://repo1.maven.org/maven2/junit/junit/maven-metadata.xml" | grep '<latest>'`
- **commons-codec**: `curl -s "https://repo1.maven.org/maven2/commons-codec/commons-codec/maven-metadata.xml" | grep '<latest>'`

### Step 6: Update pom.xml properties

Update the version properties in `pom.xml`:
- `<beam.version>`
- `<google-cloud-bigquery.version>`
- `<google-cloud-storage.version>`
- `<hadoop.version>`
- `<parquet.version>`
- Other dependencies as needed

Also update the comment referencing `libraries-bom` version.

### Step 7: Present a summary table

| Dependency | Old Version | New Version | Source |
|---|---|---|---|
| beam | 2.71.0 | 2.72.0 | Maven Central |
| libraries-bom | 26.75.0 | 26.76.0 | Beam BeamModulePlugin.groovy |
| google-cloud-bigquery | 2.58.0 | 2.59.0 | libraries-bom |
| ... | ... | ... | ... |
