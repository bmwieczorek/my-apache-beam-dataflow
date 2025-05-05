locals {
  table                            = "external_biglake_table"
  empty_parquet_file               = "myEmptyNestedRecord.parquet"
  non_empty_parquet_file           = "myNestedRecord.parquet"
  gcs_hive_zero_partition_path     = "year=0000/month=00/day=00/hour=00"
  gcs_hive_non_zero_partition_path = "year=2024/month=09/day=09/hour=09"
  bq_schema_file                   = "myNestedParquetBigQuerySchema.json"
}

data "external" "bq_connection_id_sa" {
  program = ["bash", "get_connection_id_sa.sh", var.bq_project, var.location]
}

data "external" "bq_connection_id" {
  program = ["bash", "get_connection_id.sh", var.bq_project, var.location]
}

resource "google_storage_bucket" "bucket" {
  project = var.project
  name   = "${var.project}-${var.owner}-biglake"
  location = var.location
}

resource "google_bigquery_dataset" "dataset" {
  project    = var.project
  dataset_id = "${var.owner}_biglake"
  location   = var.location
}

resource "google_storage_bucket_object" "empty_parquet_file" {
  name   = "${local.table}/${local.gcs_hive_zero_partition_path}/${local.empty_parquet_file}"
  source = local.empty_parquet_file
  bucket = google_storage_bucket.bucket.name
}

resource "null_resource" "local_parquet_metadata" {
  triggers = {
    always_run = formatdate("YYYY-MM-DD-hh-mm-ss", timestamp())
  }

  provisioner "local-exec" {
    command = "parquet meta ${local.empty_parquet_file} | grep -i time"
  }
  depends_on = [google_storage_bucket_object.empty_parquet_file]
}

resource "google_storage_bucket_iam_member" "storage_to_bq_conn_id_sa_binding" {
  bucket = google_storage_bucket.bucket.name
  role = "roles/storage.objectViewer"
  member = "serviceAccount:${data.external.bq_connection_id_sa.result.serviceAccountId}"
}

resource "google_bigquery_table" "biglake_autodetect_schema_automatic_metadata_refresh" {
  project             = var.project
  dataset_id          = google_bigquery_dataset.dataset.dataset_id
  table_id            = "${local.table}_autodetect_schema_automatic_metadata_refresh"
  deletion_protection = false
  # schema = file(local.bq_schema_file)
  schema = null
  max_staleness = "0-0 7 0:0:0" # 7 days


  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    # metadata_cache_mode = "MANUAL" # CALL BQ.REFRESH_EXTERNAL_METADATA_CACHE('myproject.test_db.test_table')
    metadata_cache_mode = "AUTOMATIC" # requires max_staleness #  googleapi: Error 400: maxStaleness must be specified when MetadataCacheMode is AUTOMATIC, invalid
    connection_id = "${var.bq_project}.us.${data.external.bq_connection_id.result.connectionId}"

    parquet_options {
      enable_list_inference = true
    }

    hive_partitioning_options {
      mode              = "CUSTOM"
      source_uri_prefix = "gs://${google_storage_bucket.bucket.name}/${local.table}/{year:STRING}/{month:STRING}/{day:STRING}/{hour:STRING}"
    }

    source_uris = ["gs://${google_storage_bucket.bucket.name}/${local.table}/*.parquet"]
  }

  depends_on = [google_storage_bucket_object.empty_parquet_file, null_resource.local_parquet_metadata]
}

resource "null_resource" "bq_show_autodetected_table_schema" {
  triggers = {
    always_run = formatdate("YYYY-MM-DD-hh-mm-ss", timestamp())
  }

  provisioner "local-exec" {
    command = "bq show --format=prettyjson ${google_bigquery_dataset.dataset.project}:${google_bigquery_dataset.dataset.dataset_id}.${google_bigquery_table.biglake_autodetect_schema_automatic_metadata_refresh.table_id} | jq '.schema.fields' | jq '.[] | select(.name==\"myRequiredDateTime\")'"
  }
  depends_on = [google_bigquery_table.biglake_autodetect_schema_automatic_metadata_refresh]
}

resource "null_resource" "bq_select_external_biglake_table_with_empty_file_only" {
  triggers = {
    always_run = formatdate("YYYY-MM-DD-hh-mm-ss", timestamp())
  }

  provisioner "local-exec" {
    command = "bq query --use_legacy_sql=false 'SELECT count(*) AS cnt FROM ${google_bigquery_dataset.dataset.project}.${google_bigquery_dataset.dataset.dataset_id}.${google_bigquery_table.biglake_autodetect_schema_automatic_metadata_refresh.table_id} WHERE year is not null'"
  }
  depends_on = [
    null_resource.bq_show_autodetected_table_schema,
    google_storage_bucket_iam_member.storage_to_bq_conn_id_sa_binding # Access Denied: bqcx-xxxx-yyy@gcp-sa-bigquery-condel.iam.gserviceaccount.com does not have storage.objects.list access to the Google Cloud Storage bucket. Permission 'storage.objects.list'. Please make sure gs://bucket/external_biglake_table/*.parquet is accessible via appropriate IAM roles, e.g. Storage Object Viewer or Storage Object Creator.
  ]
}

resource "google_storage_bucket_object" "non_empty_parquet_file" {
  name       = "${local.table}/${local.gcs_hive_non_zero_partition_path}/${local.non_empty_parquet_file}"
  source     = local.non_empty_parquet_file
  bucket     = google_storage_bucket.bucket.name
  depends_on = [null_resource.bq_select_external_biglake_table_with_empty_file_only]
}

resource "null_resource" "bq_select_external_biglake_table_with_non_empty_file" {
  triggers = {
    always_run = formatdate("YYYY-MM-DD-hh-mm-ss", timestamp())
  }

  provisioner "local-exec" {
    command = "bq query --use_legacy_sql=false 'SELECT * FROM ${google_bigquery_dataset.dataset.project}.${google_bigquery_dataset.dataset.dataset_id}.${google_bigquery_table.biglake_autodetect_schema_automatic_metadata_refresh.table_id} WHERE year is not null'"
  }

  depends_on = [null_resource.bq_select_external_biglake_table_with_empty_file_only, google_storage_bucket_object.non_empty_parquet_file]
}

resource "google_bigquery_table" "biglake_explicit_schema_manual_metadata_refresh" {
  project             = var.project
  dataset_id          = google_bigquery_dataset.dataset.dataset_id
  table_id            = "${local.table}_explicit_schema_manual_metadata_refresh"
  deletion_protection = false
  schema = file(local.bq_schema_file)
  # schema = null
  max_staleness = "0-0 7 0:0:0" # 7 days


  external_data_configuration {
    autodetect    = false
    source_format = "PARQUET"
    metadata_cache_mode = "MANUAL" # CALL BQ.REFRESH_EXTERNAL_METADATA_CACHE('myproject.test_db.test_table')
    # metadata_cache_mode = "AUTOMATIC" # requires max_staleness #  googleapi: Error 400: maxStaleness must be specified when MetadataCacheMode is AUTOMATIC, invalid
    connection_id = "${var.bq_project}.us.${data.external.bq_connection_id.result.connectionId}"

    parquet_options {
      enable_list_inference = true
    }

    hive_partitioning_options {
      mode              = "CUSTOM"
      source_uri_prefix = "gs://${google_storage_bucket.bucket.name}/${local.table}/{year:STRING}/{month:STRING}/{day:STRING}/{hour:STRING}"
    }

    source_uris = ["gs://${google_storage_bucket.bucket.name}/${local.table}/*.parquet"]
  }

  depends_on = [google_storage_bucket_object.empty_parquet_file, null_resource.local_parquet_metadata]
}

resource "null_resource" "refresh_metadata_cache" {
  triggers = {
    always_run = formatdate("YYYY-MM-DD-hh-mm-ss", timestamp())
  }

  provisioner "local-exec" {
    command = "bq query --use_legacy_sql=false 'CALL BQ.REFRESH_EXTERNAL_METADATA_CACHE(\"${google_bigquery_dataset.dataset.project}.${google_bigquery_dataset.dataset.dataset_id}.${google_bigquery_table.biglake_explicit_schema_manual_metadata_refresh.table_id}\")'"
  }

  depends_on = [google_bigquery_table.biglake_explicit_schema_manual_metadata_refresh]
}

resource "null_resource" "bq_show_explicit_table_schema" {
  triggers = {
    always_run = formatdate("YYYY-MM-DD-hh-mm-ss", timestamp())
  }

  provisioner "local-exec" {
    command = "bq show --format=prettyjson ${google_bigquery_dataset.dataset.project}:${google_bigquery_dataset.dataset.dataset_id}.${google_bigquery_table.biglake_explicit_schema_manual_metadata_refresh.table_id} | jq '.schema.fields' | jq '.[] | select(.name==\"myRequiredDateTime\")'"
  }
  depends_on = [google_bigquery_table.biglake_explicit_schema_manual_metadata_refresh]
}
