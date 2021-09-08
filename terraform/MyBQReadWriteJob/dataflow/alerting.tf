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

resource "time_sleep" "wait_100_seconds" {
  depends_on = [google_dataflow_job.my_dataflow_job]

  create_duration = "100s"
}


resource "google_monitoring_alert_policy" "my_alert_job_name" {
  depends_on = [time_sleep.wait_100_seconds]
//  depends_on = [google_dataflow_job.my_dataflow_job]
  project = var.project
  display_name = "${var.job} did not run for last 10 minutes alert policy"
  enabled = true
  combiner = "OR"

  notification_channels = [google_monitoring_notification_channel.email.name]

  documentation {
    content = "${var.job} did not run for last 10 minutes documentation"
    mime_type = "text/markdown"
  }

  conditions {
    display_name = "${var.job} did not run for last 10 minutes condition"
    condition_absent {
      filter        = "metric.type=\"dataflow.googleapis.com/job/element_count\" resource.type=\"dataflow_job\" resource.label.\"job_name\"=monitoring.regex.full_match(\"${var.job}.*\") metric.label.\"pcollection\"=\"BigQueryIO.TypedRead/ReadFiles.out0\""
      duration      = "600s" // 10 min time that a time series must violate the threshold to be considered failing
      trigger {
        percent = 100.0
      }
      aggregations {
        alignment_period = "300s" // 5 min aggregation duration, at least 60s
        cross_series_reducer = "REDUCE_SUM"
        per_series_aligner = "ALIGN_MAX"
      }
    }
  }
}

/*
resource "google_monitoring_alert_policy" "my_alert_job_id" {
  //depends_on = [time_sleep.wait_100_seconds]
  project = var.project
  display_name = "${var.job} last run (id=${google_dataflow_job.my_dataflow_job.id}) did not run for last 300s alert policy"
  enabled = true
  combiner = "OR"

  notification_channels = [google_monitoring_notification_channel.email.name]

  documentation {
    content = "${var.job} last run (id=${google_dataflow_job.my_dataflow_job.id}) did not run for last 300s documentation - please check"
    mime_type = "text/markdown"
  }

  conditions {
    display_name = "${var.job} last run (id=${google_dataflow_job.my_dataflow_job.id}) did not run for last 300s condition"
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
}*/
