resource "google_logging_project_sink" "dataflow_logging_sink" {
  project     = var.project
  name        = "dataflow-logging-sink"
  destination = "storage.googleapis.com/${var.bucket}"
  filter      = "resource.type=\"dataflow_step\" resource.labels.job_name=~\"${var.job}.*\" (logName=\"projects/${var.project}/logs/dataflow.googleapis.com%2Fjob-message\" OR logName=\"projects/${var.project}/logs/dataflow.googleapis.com%2Flauncher\") timestamp >= \"${var.dataflow_start_time}\"  severity>=INFO textPayload=\"${var.log_message_pattern}\""
  description = "Dataflow logging sink matching '${var.log_message_pattern}' pattern for job_name =~ ${var.job}.*"
  unique_writer_identity = true
}

resource "google_project_iam_binding" "log_writer" {
  project      = var.project
  role = "roles/storage.objectCreator"
  members = [
    google_logging_project_sink.dataflow_logging_sink.writer_identity
  ]
}
