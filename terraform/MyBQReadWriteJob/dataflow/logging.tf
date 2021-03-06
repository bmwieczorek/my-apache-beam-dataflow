resource "google_logging_project_sink" "dataflow-logging-sink" {
  name        = "${var.job}-logging-sink"
  destination = "storage.googleapis.com/${var.bucket}"
  project      = var.project
  filter      = "resource.type=\"dataflow_step\" resource.labels.job_name=~\"${google_dataflow_job.my_dataflow_job.name}.*\" (logName=\"projects/${var.project}/logs/dataflow.googleapis.com%2Fjob-message\" OR logName=\"projects/${var.project}/logs/dataflow.googleapis.com%2Flauncher\") timestamp >= \"${var.dataflow_start_time}\"  severity>=INFO textPayload=\"Worker pool stopped.\""
  description = "Logging sink for ${var.job}"

  unique_writer_identity = true
}

resource "google_project_iam_binding" "log-writer" {
  role = "roles/storage.objectCreator"
  project      = var.project

  members = [
    google_logging_project_sink.dataflow-logging-sink.writer_identity
  ]
}
