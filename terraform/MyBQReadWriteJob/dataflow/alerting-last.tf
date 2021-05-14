resource "google_monitoring_notification_channel" "email" {
  project = var.project
  enabled = true
  display_name = "${var.job} alert - ${var.notification_email}"
  type = "email"

  labels = {
    email_address = var.notification_email
  }

  user_labels = {
    user = var.label
  }
}

resource "google_monitoring_alert_policy" "my_alert_last" {
  project = var.project
  display_name = "${var.job} last run did not run for last 300s alert policy"
  enabled = true
  combiner = "OR"

  notification_channels = [google_monitoring_notification_channel.email.name]

  documentation {
    content = "${var.job} last run did not run for last 300s documentation - please check"
    mime_type = "text/markdown"
  }

  conditions {
    display_name = "${var.job} last run did not run for last 300s condition"
    condition_absent {
      filter        = "metric.type=\"dataflow.googleapis.com/job/element_count\" resource.type=\"dataflow_job\" metric.label.\"job_id\"=\"${google_dataflow_job.my_dataflow_job.id}\" metric.label.\"pcollection\"=\"BigQueryIO.TypedRead/ReadFiles.out0\""
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