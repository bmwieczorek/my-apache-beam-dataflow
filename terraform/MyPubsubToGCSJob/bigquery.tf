resource "google_bigquery_dataset" "dataset" {
  project                     = var.project
  dataset_id                  = local.dataset
  friendly_name               = "My dataset friendly name"
  description                 = "My dataset description"
  labels = local.labels
}

// fix Access Denied: Dataset project:bartek_mypubsubtogcsjob: Permission bigquery.datasets.get denied on dataset project:bartek_mypubsubtogcsjob (or it may not exist).",
resource "google_bigquery_dataset_iam_member" "grant_sa_data_owner_role_to_dataset" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  project = var.project
  role = "roles/bigquery.dataOwner"
  member = "serviceAccount:${var.service_account}"
  depends_on = [
    google_bigquery_dataset.dataset
  ]
}

resource "google_bigquery_table" "table" {
  project = var.project
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id = local.table
  labels = local.labels
  deletion_protection = false
  schema = <<EOF
[
  {
    "mode": "REQUIRED",
    "name": "bodyAttrsMsgId",
    "type": "STRING"
  }
]
EOF
}