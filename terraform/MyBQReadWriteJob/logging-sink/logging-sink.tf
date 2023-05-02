resource "google_logging_project_sink" "dataflow_gcs_logging_sink" {
  project     = var.project
  name        = "${var.owner}-dataflow-gcs-logging-sink"
  destination = "storage.googleapis.com/${var.bucket}"
  filter      = "resource.type=\"dataflow_step\" resource.labels.job_name=~\"${var.job}.*\" (logName=\"projects/${var.project}/logs/dataflow.googleapis.com%2Fjob-message\" OR logName=\"projects/${var.project}/logs/dataflow.googleapis.com%2Flauncher\") timestamp >= \"${var.dataflow_start_time}\"  severity>=INFO textPayload=\"${var.log_message_pattern}\""
  description = "Dataflow logging GCS sink matching '${var.log_message_pattern}' pattern for job_name =~ ${var.job}.*"
  unique_writer_identity = true
}

resource "google_project_iam_binding" "gcs_log_writer" {
  project     = var.project
  role        = "roles/storage.objectCreator"
  members     = [ google_logging_project_sink.dataflow_gcs_logging_sink.writer_identity ]
}

resource "google_logging_project_sink" "dataflow_bq_logging_sink" {
  project     = var.project
  name        = "${var.owner}-dataflow-bq-logging-sink"
  destination = "bigquery.googleapis.com/projects/${var.project}/datasets/${var.dataset}"
  filter      = "resource.type=\"dataflow_step\" resource.labels.job_name=~\"${var.job}.*\" (logName=\"projects/${var.project}/logs/dataflow.googleapis.com%2Fjob-message\" OR logName=\"projects/${var.project}/logs/dataflow.googleapis.com%2Flauncher\") timestamp >= \"${var.dataflow_start_time}\"  severity>=INFO textPayload=\"${var.log_message_pattern}\""
  description = "Dataflow logging BQ sink matching '${var.log_message_pattern}' pattern for job_name =~ ${var.job}.*"
  unique_writer_identity = true
}

resource "google_project_iam_binding" "bq_log_writer" {
  project     = var.project
  role        = "roles/bigquery.dataEditor"
  members     = [
    google_logging_project_sink.dataflow_bq_logging_sink.writer_identity
  ]
}