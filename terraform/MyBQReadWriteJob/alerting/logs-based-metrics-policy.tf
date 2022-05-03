resource "google_logging_metric" "logs_based_metric" {
  project = var.project
  name    = replace("${var.job}/log_message_counter/${var.logs_based_metrics_message_pattern}"," ", "_")
  filter  = "resource.type=dataflow_step resource.labels.job_name=~\"${var.job}.*\" logName=\"projects/${var.project}/logs/dataflow.googleapis.com%2Fworker\" severity>=DEBUG \"${var.logs_based_metrics_message_pattern}\""
  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
  }
}

resource "google_monitoring_alert_policy" "logs_based_metric_policy" {
  project      = var.project
  display_name = "${var.job} did not created 3 log entries in last 15 min for '${var.logs_based_metrics_message_pattern}' pattern policy"
  enabled      = true
  combiner     = "OR"

//  notification_channels = toset([for k in google_monitoring_notification_channel.email: k.name])
  notification_channels = [google_monitoring_notification_channel.email.name]

  documentation {
    content    = "${var.job} did not created 3 log entries in last 15 min for for '${var.logs_based_metrics_message_pattern}' pattern documentation"
    mime_type  = "text/markdown"
  }

  conditions {
    display_name      = "${var.job} did not created 3 log entries in last 15 min for '${var.logs_based_metrics_message_pattern}' pattern condition"
    condition_threshold {
      filter          = "metric.type=\"logging.googleapis.com/user/${google_logging_metric.logs_based_metric.name}\" resource.type=\"dataflow_job\""
      duration        = "900s"  // 15 min time that a time series must violate the threshold to be considered failing
      comparison      = "COMPARISON_LT"
      threshold_value = 3
      aggregations {
        alignment_period = "180s" // 3 min aggregation duration, at least 60s
        per_series_aligner = "ALIGN_SUM"
      }
    }
  }
}