locals {
  ts = formatdate("YYYYMMDDhhmmss", timestamp())
  labels = {
    owner = var.owner
  }
}

resource "google_bigquery_dataset" "my_dataset" {
  project                     = var.project
  dataset_id                  = var.dataset
  friendly_name               = "My dataset friendly name"
  description                 = "My dataset description"
  labels = local.labels
}

resource "google_bigquery_table" "my_table" {
  project    = var.project
  dataset_id = google_bigquery_dataset.my_dataset.dataset_id
  table_id   = var.table
  labels = local.labels
  deletion_protection = false
  schema = file(var.table_schema_file)
/*
  schema = <<EOF
[
  {
    "mode": "REQUIRED",
    "name": "id",
    "type": "STRING",
    "description" : "The id"
  },
  {
    "mode": "REQUIRED",
    "name": "creation_timestamp",
    "type": "TIMESTAMP",
    "description" : "The creation timestamp"
  },
  {
    "mode": "REQUIRED",
    "name": "expiration_date",
    "type": "DATE",
    "description" : "The expiration date"
  }
]
EOF
*/

}

resource "google_storage_bucket_object" "my_load_file" {
  name   = "bigquery/${var.load_file}"
  source = "bigquery/${var.load_file}"
  bucket = var.bucket
}

resource "google_bigquery_job" "my_bigquery_job" {
  project = var.project
  job_id  = "my_bigquery_job_${local.ts}"
  labels  = local.labels

  load {
    source_uris = [
      "gs://${google_storage_bucket_object.my_load_file.bucket}/${google_storage_bucket_object.my_load_file.name}"
    ]
    source_format = "NEWLINE_DELIMITED_JSON"

    destination_table {
      project_id = var.project
      dataset_id = google_bigquery_table.my_table.dataset_id
      table_id = google_bigquery_table.my_table.table_id
    }
    write_disposition = "WRITE_TRUNCATE"
  }

//  query {
//    query = "INSERT INTO ${google_bigquery_dataset.my_dataset.dataset_id}.${google_bigquery_table.my_table.table_id} (id,creation_timestamp, expiration_date, numbers) values(\"abc\",TIMESTAMP(\"2021-03-03 03:03:03+00\"),DATE '2021-03-03',[1,2,3]),(\"def\",CURRENT_TIMESTAMP(),CURRENT_DATE(),[5,6,7])"
//    create_disposition = ""
//    write_disposition = ""
//  }
}