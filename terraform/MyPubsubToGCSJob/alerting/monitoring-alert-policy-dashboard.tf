resource "google_monitoring_dashboard" "alerting_dashboard_job_name_all" {
  project = var.project
  dashboard_json = templatefile("${path.module}/${var.dashboard_file}", {
    dashboard_name = "${var.job_name} alerts dashboard"
    policy1 = google_monitoring_alert_policy.job_name_no_reducer_all_series_policy.id
    policy2 = google_monitoring_alert_policy.job_name_regex_no_reducer_all_series_policy.id
    policy3 = google_monitoring_alert_policy.job_name_no_reducer_any_series_policy.id
    policy4 = google_monitoring_alert_policy.job_name_regex_no_reducer_any_series_policy.id
    policy5 = google_monitoring_alert_policy.job_name_sum_reducer_all_series_policy.id
    policy6 = google_monitoring_alert_policy.job_name_regex_sum_reducer_all_series_policy.id
    policy7 = google_monitoring_alert_policy.job_name_sum_reducer_any_series_policy.id
    policy8 = google_monitoring_alert_policy.job_name_regex_sum_reducer_any_series_policy.id
    policy9 = google_monitoring_alert_policy.job_id_policy.id
  })
}
