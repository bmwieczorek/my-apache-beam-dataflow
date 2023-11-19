locals {
  external_table_old = "external_table_old"
  external_table_new = "external_table_new"
  parquet_file_old   = "myNestedRecordFewerFields.parquet"
  parquet_file_new   = "myNestedRecordMoreFields.parquet"
  schema_old_path    = "schema_old.json"
  schema_new_path    = "schema_new.json"
  gcs_hive_old_path  = "year=2023/month=01/day=01/hour=00"
  gcs_hive_new_path  = "year=2023/month=01/day=01/hour=01"
  ts                 = formatdate("YYYYMMDDhhmmss", timestamp())
}


resource "google_storage_bucket_object" "parquet_file_old" {
  name   = "${local.external_table_old}/${local.gcs_hive_old_path}/${local.parquet_file_old}"
  source = local.parquet_file_old
  bucket = var.bucket
}


resource "google_bigquery_table" "external_table_old" {
  project             = var.project
  dataset_id          = var.dataset_id
  table_id            = local.external_table_old
  deletion_protection = false
  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"

    parquet_options {
      enable_list_inference = true
    }

    hive_partitioning_options {
      mode              = "CUSTOM"
      source_uri_prefix = "gs://${var.bucket}/${local.external_table_old}/{year:STRING}/{month:STRING}/{day:STRING}/{hour:STRING}"
    }

    source_uris = ["gs://${var.bucket}/${local.external_table_old}/*.parquet"]
  }
  depends_on = [google_storage_bucket_object.parquet_file_old]
}


resource "null_resource" "bq_select_external_table_old" {
  triggers = {
    always_run = formatdate("YYYY-MM-DD-hh-mm-ss", timestamp())
  }

  provisioner "local-exec" {
    command = "bq query --use_legacy_sql=false 'SELECT * FROM `${var.dataset_id}.${google_bigquery_table.external_table_old.table_id}`'"
  }

}


resource "null_resource" "bq_get_schema_external_table_old" {
  triggers = {
    always_run = formatdate("YYYY-MM-DD-hh-mm-ss", timestamp())
  }

  provisioner "local-exec" {
    command = "bq show --schema --format=prettyjson ${var.project}:${var.dataset_id}.${google_bigquery_table.external_table_old.table_id} | jq 'del(.[] | select((.name == \"year\") or (.name == \"month\") or (.name == \"day\") or (.name == \"hour\")))' > ${local.schema_old_path}"
  }

}


resource "google_storage_bucket_object" "parquet_file_new" {
  name   = "${local.external_table_new}/${local.gcs_hive_new_path}//${local.parquet_file_new}"
  source = local.parquet_file_new
  bucket = var.bucket
}


resource "google_bigquery_table" "external_table_new" {
  project             = var.project
  dataset_id          = var.dataset_id
  table_id            = local.external_table_new
  deletion_protection = false
  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"

    parquet_options {
      enable_list_inference = true
    }

    hive_partitioning_options {
      mode              = "CUSTOM"
      source_uri_prefix = "gs://${var.bucket}/${local.external_table_new}/{year:STRING}/{month:STRING}/{day:STRING}/{hour:STRING}"
    }

    source_uris = ["gs://${var.bucket}/${local.external_table_new}/*.parquet"]
  }
  depends_on = [google_storage_bucket_object.parquet_file_new]
}


resource "null_resource" "bq_select_external_table_new" {
  triggers = {
    always_run = formatdate("YYYY-MM-DD-hh-mm-ss", timestamp())
  }

  provisioner "local-exec" {
    command = "bq query --use_legacy_sql=false 'SELECT * FROM `${var.dataset_id}.${google_bigquery_table.external_table_new.table_id}`'"
  }

}


resource "null_resource" "bq_get_schema_external_table_new" {
  triggers = {
    always_run = formatdate("YYYY-MM-DD-hh-mm-ss", timestamp())
  }

  provisioner "local-exec" {
    command = "bq show --schema --format=prettyjson ${var.project}:${var.dataset_id}.${google_bigquery_table.external_table_new.table_id} | jq 'del(.[] | select((.name == \"year\") or (.name == \"month\") or (.name == \"day\") or (.name == \"hour\")))' > ${local.schema_new_path}"
  }

}
