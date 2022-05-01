output "logs_based_metric_type" {
  value = "logging.googleapis.com/user/${google_logging_metric.logs_based_metric.name}"
}
