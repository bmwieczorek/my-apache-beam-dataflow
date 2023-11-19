locals {
  name               = "${var.owner}-external-table"
  bucket             = "${var.project}-${local.name}"
  dataset            = replace(local.name, "-", "_")
  external_table     = "external_table"
  external_table_old = "${local.external_table}_old"
  external_table_new = "${local.external_table}_new"
  native_table       = "native_table"
  #  file               = "myNestedRecord.parquet"
  parquet_file_old   = "myNestedRecordFewerFields.parquet"
  parquet_file_new   = "myNestedRecordMoreFields.parquet"
  #  schema_file        = "schema.json"
  schema_file        = "schema_old.json"
  #  schema_file        = "schema_new.json"
  ts                 = formatdate("YYYYMMDDhhmmss", timestamp())
  partition          = "2023111415"

}


module "individual_external_tables" {
  source              = "./individual_external_tables"
  project             = var.project
  owner               = var.owner
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  bucket = google_storage_bucket.bucket.name
}

module "external_table" {
  source              = "./external_table"
  project             = var.project
  owner               = var.owner
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  bucket = google_storage_bucket.bucket.name
  schema_file = var.schema_file
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

resource "google_bigquery_dataset" "dataset" {
  project       = var.project
  dataset_id    = local.dataset
  friendly_name = local.dataset
  location      = "US"

  labels = {
    owner = var.owner
  }
}