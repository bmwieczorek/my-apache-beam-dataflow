resource "time_sleep" "wait_180_seconds" {
  depends_on = [var.module_depends_on]
  create_duration = "180s"
}

resource "google_monitoring_alert_policy" "job_name_policy" {
  depends_on = [time_sleep.wait_180_seconds]
  project = var.project
  display_name = "${var.job} did not run for last 15 minutes alert policy"
  enabled = true
  combiner = "OR"

//  notification_channels = toset([for k in google_monitoring_notification_channel.email: k.name])
  notification_channels = [google_monitoring_notification_channel.email.name]

  documentation {
    content = "${var.job} did not run for last 15 minutes documentation"
    mime_type = "text/markdown"
  }

  conditions {
    display_name = "${var.job} did not run for last 15 minutes condition"
    condition_absent {
      filter        = "metric.type=\"dataflow.googleapis.com/job/element_count\" resource.type=\"dataflow_job\" resource.label.\"job_name\"=monitoring.regex.full_match(\"${var.job}.*\") metric.label.\"pcollection\"=\"BigQueryIO.TypedRead/ReadFiles.out0\""
      duration      = "900s" // 15 min time that a time series must violate the threshold to be considered failing
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

//  notification_channels = toset([for k in google_monitoring_notification_channel.email: k.name])
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

//gcloud beta monitoring channels list --filter='type="email" AND displayName="Notification channel a@example.com"' --format 'value(NAME)'
//gcloud beta monitoring channels delete $(gcloud beta monitoring channels list --filter='type="email" AND displayName="Notification channel a@example.com"' --format 'value(NAME)')

//gcloud alpha monitoring policies list --filter='displayName="my-job did not run for last 15 minutes alert policy"' --format=json
//gcloud alpha monitoring policies list --filter='displayName="my-job did not run for last 15 minutes alert policy"' --format 'value(NAME)'

//gcloud alpha monitoring policies update "projects/${GCP_PROJECT}/alertPolicies/8823035245391683282" --remove-notification-channels "projects/${GCP_PROJECT}/notificationChannels/1224431537736163531"
//gcloud alpha monitoring policies update $(gcloud alpha monitoring policies list --filter='displayName="my-job did not run for last 15 minutes alert policy"' --format 'value(NAME)') --remove-notification-channels $(gcloud beta monitoring channels list --filter='type="email" AND displayName="Notification channel a@example.com"' --format 'value(NAME)')
