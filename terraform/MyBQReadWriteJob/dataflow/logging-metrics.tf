resource "google_logging_metric" "logging_metric" {
  project = var.project
  name   = "${var.job}-mysubscription-counter"
  filter = "resource.type=dataflow_step resource.labels.job_name=~\"${var.job}.*\" logName=\"projects/${var.project}/logs/dataflow.googleapis.com%2Fworker\" severity>=DEBUG \"Created MySubscription\""
  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
  }
}

resource "google_monitoring_alert_policy" "my_alert_logging_metrics_mysubscription_counter" {
  //  depends_on = [time_sleep.wait_100_seconds]
  //  depends_on = [google_dataflow_job.my_dataflow_job]
  project = var.project
  display_name = "${var.job} did not created 3 log entries in last 5 min for created mysubscriptions policy"
  enabled = false
  combiner = "OR"

  notification_channels = [google_monitoring_notification_channel.email.name]

  documentation {
    content = "${var.job} did not created 3 log entries in last 5 min for created mysubscriptions documentation - please check"
    mime_type = "text/markdown"
  }

  conditions {
    display_name = "${var.job} did not created 3 log entries in last 5 min for created mysubscriptions condition"
    condition_threshold {
      filter        = "metric.type=\"logging.googleapis.com/user/${google_logging_metric.logging_metric.name}\" resource.type=\"dataflow_job\""
      duration      = "300s"  // 5 min time that a time series must violate the threshold to be considered failing
      comparison = "COMPARISON_LT"
      threshold_value = 3
      aggregations {
        alignment_period = "180s" // 3 min aggregation duration, at least 60s
        per_series_aligner = "ALIGN_SUM"
      }
    }
  }

}