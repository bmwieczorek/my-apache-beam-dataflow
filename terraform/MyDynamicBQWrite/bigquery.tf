locals {
  labels = {
    owner = var.owner
  }
}

resource "google_bigquery_dataset" "dataset" {
  project                     = var.project
  dataset_id                  = "${var.owner}_mydynamicbqwritejob"

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
    },
    {
      "mode": "NULLABLE",
      "name": "myOptionalNumeric",
      "type": "NUMERIC"
    },
    {
      "mode": "NULLABLE",
      "name": "myOptionalDouble",
      "type": "FLOAT"
    },
    {
      "mode": "NULLABLE",
      "name": "myOptionalTime",
      "type": "TIME"
    },
    {
      "mode": "NULLABLE",
      "name": "myOptionalBytes",
      "type": "BYTES"
    },
    {
      "mode": "NULLABLE",
      "name": "myOptionalFloat",
      "type": "FLOAT"
    },
    {
      "mode": "NULLABLE",
      "name": "myOptionalLong",
      "type": "INTEGER"
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
      "mode": "REQUIRED",
      "name": "myRequiredBoolean",
      "type": "BOOLEAN"
    },
    {
      "mode": "REQUIRED",
      "name": "myRequiredNumeric",
      "type": "NUMERIC"
    },
    {
      "mode": "REQUIRED",
      "name": "myRequiredDouble",
      "type": "FLOAT"
    },
    {
      "mode": "REQUIRED",
      "name": "myRequiredTime",
      "type": "TIME"
    },
    {
      "mode": "REQUIRED",
      "name": "myRequiredBytes",
      "type": "BYTES"
    },
    {
      "mode": "REQUIRED",
      "name": "myRequiredFloat",
      "type": "FLOAT"
    },
    {
      "mode": "REQUIRED",
      "name": "myRequiredLong",
      "type": "INTEGER"
    }
  ]
  EOF

}