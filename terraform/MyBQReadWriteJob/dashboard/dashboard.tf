resource "google_monitoring_dashboard" "dashboard" {
  project = var.project
  dashboard_json = templatefile(var.dashboard_file, {
    job = var.job
  })
}
