locals {
  external_table     = "external_table"
  ts                 = formatdate("YYYYMMDDhhmmss", timestamp())
}

resource "google_bigquery_table" "external_table" {
  project             = var.project
  dataset_id          = var.dataset_id
  table_id            = local.external_table
  deletion_protection = false
  schema              = file(var.schema_file)

  external_data_configuration {
    autodetect    = false
    source_format = "PARQUET"

    parquet_options {
      enable_list_inference = true
    }

    hive_partitioning_options {
      mode              = "CUSTOM"
      source_uri_prefix = "gs://${var.bucket}/${local.external_table}/{year:STRING}/{month:STRING}/{day:STRING}/{hour:STRING}"
    }

    source_uris = ["gs://${var.bucket}/${local.external_table}/*.parquet"]
  }
}
