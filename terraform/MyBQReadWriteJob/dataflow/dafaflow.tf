resource "google_storage_bucket_object" "my_bucket_object" {
  name   = "templates/${var.job}-template_metadata"
  source = "../../../dataflow-templates/${var.job}-template_metadata"
  bucket = var.bucket
}

resource "google_dataflow_job" "my_dataflow_job" {
  project               = var.project
  name                  = "${var.job}-${var.label}-template-${var.expiration_date}"
  temp_gcs_location     = "gs://${var.bucket}/temp"
  template_gcs_path     = "gs://${var.bucket}/templates/${var.job}-template"
  service_account_email = var.service_account
  subnetwork            = var.subnetwork
  max_workers           = 3
  on_delete             = "cancel"
  ip_configuration      = "WORKER_IP_PRIVATE"
  region                = var.region
  machine_type          = "e2-small"
  parameters = {
    expirationDate = var.expiration_date
  }
  labels = {
    app   = var.label
  }
}
