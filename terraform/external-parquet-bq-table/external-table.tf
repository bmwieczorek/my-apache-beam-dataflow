locals {
  name    = "${var.owner}-external-table"
  bucket  = "${var.project}-${local.name}"
  dataset = replace(local.name, "-", "_")
  table   = "external_table"
  file    = "myNestedRecord.parquet"
}

resource "google_storage_bucket" "bucket" {
  project       = var.project
  name          = local.bucket
  location      = "US"
  force_destroy = true
  labels        = {
    owner = var.owner
  }
}

resource "google_storage_bucket_object" "parquet_file1" {
  name    = "year=2023/month=01/day=01/hour=00/${local.file}"
  source  = local.file
  bucket  = google_storage_bucket.bucket.name
}

resource "google_storage_bucket_object" "parquet_file2" {
  name    = "year=2023/month=01/day=01/hour=01/${local.file}"
  source  = local.file
  bucket  = google_storage_bucket.bucket.name
}

resource "google_bigquery_dataset" "dataset" {
  project       = var.project
  dataset_id    = local.dataset
  friendly_name = local.dataset
  location      = "US"

  labels = {
    owner = var.owner
  }
}

resource "google_bigquery_table" "external_table" {
  project             = var.project
  dataset_id          = google_bigquery_dataset.dataset.dataset_id
  table_id            = local.table
  deletion_protection = false
  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"

    parquet_options {
      enable_list_inference = true
    }

    hive_partitioning_options {
      mode              = "CUSTOM"
      source_uri_prefix = "gs://${google_storage_bucket.bucket.name}/{year:STRING}/{month:STRING}/{day:STRING}/{hour:STRING}"
    }

    source_uris = ["gs://${google_storage_bucket.bucket.name}/*.parquet"]
  }
  depends_on = [google_storage_bucket_object.parquet_file1, google_storage_bucket_object.parquet_file2]
}

resource "null_resource" "bq_select_table" {
  triggers = {
    always_run = formatdate("YYYY-MM-DD-hh-mm-ss", timestamp())
  }

  provisioner "local-exec" {
    command = "bq query --use_legacy_sql=false 'SELECT * FROM `${local.dataset}.${local.table}` LIMIT 1'"
  }

  depends_on = [google_bigquery_table.external_table]
}
