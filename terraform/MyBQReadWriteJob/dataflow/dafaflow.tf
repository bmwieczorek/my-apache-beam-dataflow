resource "google_storage_bucket_object" "my_bucket_object" {
  name   = "templates/${var.job}-template_metadata"
  source = "../../../dataflow-templates/${var.job}-template_metadata"
  bucket = var.bucket
}

resource "google_dataflow_job" "my_dataflow_job" {
  depends_on = [google_compute_instance.compute_template]
  project               = var.project
  name                  = "${var.job}-template-${var.expiration_date}"
  temp_gcs_location     = "gs://${var.bucket}/temp"
  template_gcs_path     = "gs://${var.bucket}/templates/${var.job}-template"
  service_account_email = var.service_account
  subnetwork            = var.subnetwork
  max_workers           = 3
  on_delete             = "cancel"
  ip_configuration      = "WORKER_IP_PRIVATE"
  region                = var.region
  machine_type          = "n1-standard-1"
  parameters = {
    expirationDate = var.expiration_date
  }
  additional_experiments = ["enable_stackdriver_agent_metrics"]
  labels = {
    app   = var.label
  }
}
