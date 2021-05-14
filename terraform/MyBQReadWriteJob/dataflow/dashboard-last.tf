resource "google_monitoring_dashboard" "dashboard-last" {
  project = var.project
  dashboard_json = templatefile(var.dashboard_file, {
    job = var.job
    job_id = google_dataflow_job.my_dataflow_job.id
  })
}
