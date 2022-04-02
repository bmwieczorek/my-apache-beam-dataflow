resource "google_logging_project_sink" "gcs_logging_sink" {
  project     = var.project
  name        = "gcs-logging-sink"
  destination = "storage.googleapis.com/${local.bucket}"
  filter      = "resource.type=\"gce_subnetwork\" log_name=\"projects/${var.project}/logs/compute.googleapis.com%2Ffirewall\" jsonPayload.instance.vm_name=\"${local.worker_vm}\" jsonPayload.remote_instance.vm_name=\"${local.curl_vm}\""
  description = "${local.name} GCS logging sink"
  unique_writer_identity = true
}

resource "google_pubsub_topic" "topic" {
  project    = var.project
  name       = "${local.name}-topic"
  labels     = local.labels
}
resource "google_pubsub_subscription" "subscription" {
  project = var.project
  name    = "${local.name}-subscription"
  topic   = google_pubsub_topic.topic.name
  labels  = local.labels
}



resource "google_logging_project_sink" "pubsub_logging_sink" {
  project     = var.project
  name        = "${local.name}-pubsub-logging-sink"
  destination = "pubsub.googleapis.com/projects/${var.project}/topics/${google_pubsub_topic.topic.name}"
  filter      = "resource.type=\"gce_subnetwork\" log_name=\"projects/${var.project}/logs/compute.googleapis.com%2Ffirewall\" jsonPayload.instance.vm_name=\"${local.worker_vm}\" jsonPayload.remote_instance.vm_name=\"${local.curl_vm}\""
  description = "${local.name} PubSub logging sink"
  unique_writer_identity = true
}

resource "google_project_iam_binding" "gcs_log_writer" {
  project     = var.project
  role        = "roles/storage.objectCreator"
  members     = [ google_logging_project_sink.gcs_logging_sink.writer_identity ]
}

resource "google_project_iam_binding" "pubsub_log_writer" {
  project     = var.project
  role        = "roles/pubsub.publisher"
  members     = [ google_logging_project_sink.pubsub_logging_sink.writer_identity ]
}



resource "google_bigquery_dataset" "dataset" {
  project                    = var.project
  dataset_id                 = "${local.name}_dataset"
  delete_contents_on_destroy = true
}

resource "google_logging_project_sink" "bq_logging_sink" {
  project     = var.project
  name        = "${local.name}-bq-logging-sink"
  destination = "bigquery.googleapis.com/projects/${var.project}/datasets/${google_bigquery_dataset.dataset.dataset_id}"
  filter      = "resource.type=\"gce_subnetwork\" log_name=\"projects/${var.project}/logs/compute.googleapis.com%2Ffirewall\" jsonPayload.instance.vm_name=\"${local.worker_vm}\" jsonPayload.remote_instance.vm_name=\"${local.curl_vm}\""
  description = "${local.name} BQ logging sink"
  unique_writer_identity = true
}

resource "google_project_iam_binding" "bq_log_writer" {
  project     = var.project
  role        = "roles/bigquery.dataEditor"
  members     = [ google_logging_project_sink.bq_logging_sink.writer_identity ]
}
