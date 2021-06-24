resource "google_monitoring_notification_channel" "email" {
  project = var.project
  enabled = true
  display_name = "${var.job} alert - ${var.notification_email}"
  type = "email"

  labels = {
    email_address = var.notification_email
  }

  user_labels = {
    owner = var.owner
  }
}

resource "google_monitoring_alert_policy" "my_alert" {
  project = var.project
  display_name = "${var.job} not run for last 300s alert policy"
  enabled = true
  combiner = "OR"

  notification_channels = [google_monitoring_notification_channel.email.name]

  documentation {
    content = "${var.job} not run for last 300s documentation - please check"
    mime_type = "text/markdown"
  }

  conditions {
    display_name = "${var.job} not run for last 300s condition"
    condition_absent {
      filter        = "metric.type=\"dataflow.googleapis.com/job/element_count\" resource.type=\"dataflow_job\" resource.label.\"job_name\"=monitoring.regex.full_match(\"${var.job}.*\") metric.label.\"pcollection\"=\"BigQueryIO.TypedRead/ReadFiles.out0\""
      duration      = "300s"
      trigger {
        percent = 100.0
      }
      aggregations {
        alignment_period = "300s"
        per_series_aligner = "ALIGN_MEAN"
      }
    }
  }
}