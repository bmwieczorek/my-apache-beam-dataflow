resource "google_monitoring_dashboard" "alerting_dashboard_job_name_all" {
  project = var.project
  dashboard_json = templatefile("${path.module}/${var.dashboard_file}", {
    dashboard_name = "${var.job_name} alert dashboard"
    policy = google_monitoring_alert_policy.job_name_regex_sum_reducer_all_series_policy.id
  })
}
