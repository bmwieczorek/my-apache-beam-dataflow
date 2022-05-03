locals {
  ts = formatdate("YYYYMMDDhhmmss", timestamp())
  owner = var.owner

  project_level_roles = ["roles/bigquery.user"]
//  project_level_roles = ["roles/bigquery.jobUser"]
//  project_level_roles = []

//  dataset_level_roles = ["roles/bigquery.dataViewer"]
  dataset_level_roles = ["roles/bigquery.dataEditor"]
//  dataset_level_roles = []

//   project role  /  dataset role       |  bq ls                                            | bq head                                             | bq query select                                                | bq query insert                                               | bq query delete
//               - / -                   | Permission bigquery.tables.list denied on dataset | Permission bigquery.tables.getData denied on table  | User does not have bigquery.jobs.create permission in project
//               - / bigquery.dataViewer | OK                                                | OK                                                  | User does not have bigquery.jobs.create permission in project
//bigquery.user    / -                   | OK                                                | Permission bigquery.tables.getData denied on table  | User does not have permission to query table                   | User does not have permission to query table
//bigquery.user    / bigquery.dataViewer | OK                                                | OK                                                  | OK                                                             | Permission bigquery.tables.updateData denied on table
//bigquery.jobUser / -                   | Permission bigquery.tables.list denied on dataset | Permission bigquery.tables.getData denied on table  | User does not have permission to query table
//bigquery.jobUser / bigquery.dataViewer | OK                                                | OK                                                  | OK                                                             | Permission bigquery.tables.updateData denied on table
//bigquery.user    / bigquery.dataEditor | OK                                                | OK                                                  | OK                                                             | OK
//               - / bigquery.dataEditor | OK                                                | OK                                                  | User does not have bigquery.jobs.create permission in project  | User does not have bigquery.jobs.create permission in project | OK

}

resource "google_bigquery_dataset" "private_dataset" {
  project = var.project
  dataset_id = "${var.owner}_private_dataset"
}

resource "google_service_account" "sa" {
  project      = var.project
  account_id   = "${local.owner}-sa"
}

resource "google_project_iam_member" "project_level_roles_for_sa" {
  for_each = toset(local.project_level_roles)
  project  = var.project
  role     = each.value
  member   = "serviceAccount:${google_service_account.sa.email}"
}

resource "google_bigquery_dataset_access" "private_dataset_level_roles_for_sa" {
  for_each       = toset(local.dataset_level_roles)
  project        = var.project
  dataset_id     = google_bigquery_dataset.private_dataset.dataset_id
  role           = each.value
  user_by_email  = google_service_account.sa.email
}

resource "google_bigquery_dataset" "public_dataset" {
  project = var.project
  dataset_id = "${var.owner}_public_dataset"
}

// optional to see that the sa can query public dataset view even without view to dataset or dataset to dataset access since sa have access to both datasets
resource "google_bigquery_dataset_access" "public_dataset_level_roles_for_sa" {
  for_each       = toset(local.dataset_level_roles)
  project        = var.project
  dataset_id     = google_bigquery_dataset.public_dataset.dataset_id
  role           = each.value
  user_by_email  = google_service_account.sa.email
}

resource "google_bigquery_dataset_access" "dataset_to_dataset_access" {
  project       = var.project
  dataset_id    = google_bigquery_dataset.private_dataset.dataset_id

  dataset {
    dataset{
      project_id = var.project
      dataset_id = google_bigquery_dataset.public_dataset.dataset_id
    }
    target_types = ["VIEWS"]
  }
}

resource "google_service_account" "other_sa" {
  project      = var.project
  account_id   = "${local.owner}-other-sa"
}

resource "google_project_iam_member" "project_level_roles_for_other_sa" {
  for_each = toset(local.project_level_roles)
  project  = var.project
  role     = each.value
  member   = "serviceAccount:${google_service_account.other_sa.email}"
}

resource "google_bigquery_dataset_access" "public_dataset_level_roles_for_other_sa" {
  for_each       = toset(local.dataset_level_roles)
  project        = var.project
  dataset_id     = google_bigquery_dataset.public_dataset.dataset_id
  role           = each.value
  user_by_email  = google_service_account.other_sa.email
}
