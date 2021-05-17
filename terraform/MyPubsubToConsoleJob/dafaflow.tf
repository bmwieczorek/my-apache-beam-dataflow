resource "google_storage_bucket_object" "my_bucket_object" {
  name   = "templates/app-image-spec.json"
  source = "../../target/classes/flex-templates/app-image-spec.json"
  bucket = google_storage_bucket.my_bucket.id
}

resource "google_dataflow_flex_template_job" "my_dataflow_flex_job" {
  project               = var.project
  provider              = google-beta
  name                  = "${var.job}-flex-template"
  container_spec_gcs_path = "gs://${var.bucket}/templates/app-image-spec.json"
  on_delete             = "cancel"
  region                = var.region
  parameters = {
    subscription = google_pubsub_subscription.my_subscription.id
    output = var.output
    workerMachineType = "n2-standard-2"
    workerDiskType = "compute.googleapis.com/projects/${var.project}/zones/us-central1-c/diskTypes/pd-standard"
    diskSizeGb = 200
    serviceAccount = var.service_account
    subnetwork = var.subnetwork
    usePublicIps = false
    experiments = "enable_stackdriver_agent_metrics"
  }
}