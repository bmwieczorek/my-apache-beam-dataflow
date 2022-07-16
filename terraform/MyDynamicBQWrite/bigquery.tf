locals {
  labels = {
    owner = var.owner
  }
}

resource "google_bigquery_dataset" "dataset" {
  project                     = var.project
  dataset_id                  = "${var.owner}_mydynamicbqwrite"

  labels = local.labels
}

resource "google_bigquery_table" "optional_record" {
  project    = var.project
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = "optional_record"
  labels = local.labels
  deletion_protection = false
  schema = <<EOF
  [
    {
      "mode": "NULLABLE",
      "name": "myOptionalString",
      "type": "STRING"
    },
    {
      "mode": "NULLABLE",
      "name": "myOptionalInt",
      "type": "INTEGER"
    },
    {
      "mode": "NULLABLE",
      "name": "myOptionalDate",
      "type": "DATE"
    },
    {
      "mode": "NULLABLE",
      "name": "myOptionalTimestamp",
      "type": "TIMESTAMP"
    },
    {
      "mode": "NULLABLE",
      "name": "myOptionalBoolean",
      "type": "BOOLEAN"
    }
  ]
  EOF

}


resource "google_bigquery_table" "required_record" {
  project    = var.project
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = "required_record"
  labels = local.labels
  deletion_protection = false
  schema = <<EOF
  [
    {
      "mode": "REQUIRED",
      "name": "myRequiredString",
      "type": "STRING"
    },
    {
      "mode": "REQUIRED",
      "name": "myRequiredInt",
      "type": "INTEGER"
    },
    {
      "mode": "REQUIRED",
      "name": "myRequiredDate",
      "type": "DATE"
    },
    {
      "mode": "REQUIRED",
      "name": "myRequiredTimestamp",
      "type": "TIMESTAMP"
    },
    {
      "mode": "NULLABLE",
      "name": "myOptionalBoolean",
      "type": "BOOLEAN"
    },
    {
      "mode": "REQUIRED",
      "name": "myRequiredBoolean",
      "type": "BOOLEAN"
    }
  ]
  EOF

}