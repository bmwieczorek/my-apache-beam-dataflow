resource "time_sleep" "wait_180_seconds" {
  depends_on = [var.module_depends_on]
  create_duration = "180s"
}

resource "google_monitoring_alert_policy" "job_name_no_reducer_all_series_policy" {
  depends_on = [time_sleep.wait_180_seconds]
  project = var.project
  display_name = "${var.job_name} 3m job name no reducer all series policy"
  enabled = true
  combiner = "OR"
  notification_channels = [google_monitoring_notification_channel.email.name]
  documentation {
    content = "${var.job_name} 3m job name no reducer all series documentation"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "${var.job_name} 3m job name no reducer all series condition"
    condition_threshold {
      filter        = "metric.type=\"dataflow.googleapis.com/job/elements_produced_count\" resource.type=\"dataflow_job\" resource.label.\"job_name\"=\"${var.job_name}\" metric.label.\"pcollection\"=\"PubsubIO.Read/MapElements/Map.out0\""
      duration      = "180s" // 3 min time that a time series must violate the threshold to be considered failing
      comparison    = "COMPARISON_LT"
      threshold_value = "0.1"
      trigger {
        percent = 100.0
      }
      aggregations {
        alignment_period = "60s" // 5 min aggregation duration, at least 60s
        //        cross_series_reducer = "REDUCE_SUM"
        per_series_aligner = "ALIGN_RATE" // graph - 0 if job running, absent if job not running
      }
    }
  }

// google provider v4
  alert_strategy {
    auto_close = "1800s"
  }
}

resource "google_monitoring_alert_policy" "job_name_regex_no_reducer_all_series_policy" {
  depends_on = [time_sleep.wait_180_seconds]
  project = var.project
  display_name = "${var.job_base_name}.* 3m job name no reducer all series policy"
  enabled = true
  combiner = "OR"
  notification_channels = [google_monitoring_notification_channel.email.name]
  documentation {
    content = "${var.job_base_name}.* 3m job name no reducer all series documentation"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "${var.job_base_name}.* 3m job name no reducer all series condition"
    condition_threshold {
      filter        = "metric.type=\"dataflow.googleapis.com/job/elements_produced_count\" resource.type=\"dataflow_job\" resource.label.\"job_name\"=monitoring.regex.full_match(\"${var.job_base_name}.*\") metric.label.\"pcollection\"=\"PubsubIO.Read/MapElements/Map.out0\""
      duration      = "180s" // 3 min time that a time series must violate the threshold to be considered failing
      comparison    = "COMPARISON_LT"
      threshold_value = "0.1"
      trigger {
        percent = 100.0
      }
      aggregations {
        alignment_period = "60s" // 5 min aggregation duration, at least 60s
        //        cross_series_reducer = "REDUCE_SUM"
        per_series_aligner = "ALIGN_RATE" // graph - 0 if job running, absent if job not running
      }
    }
  }
}

resource "google_monitoring_alert_policy" "job_name_no_reducer_any_series_policy" {
  depends_on = [time_sleep.wait_180_seconds]
  project = var.project
  display_name = "${var.job_name} 3m job name no reducer any series policy"
  enabled = true
  combiner = "OR"
  notification_channels = [google_monitoring_notification_channel.email.name]
  documentation {
    content = "${var.job_name} 3m job name no reducer any series documentation"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "${var.job_name} 3m job name no reducer any series condition"
    condition_threshold {
      filter        = "metric.type=\"dataflow.googleapis.com/job/elements_produced_count\" resource.type=\"dataflow_job\" resource.label.\"job_name\"=\"${var.job_name}\" metric.label.\"pcollection\"=\"PubsubIO.Read/MapElements/Map.out0\""
      duration      = "180s" // 3 min time that a time series must violate the threshold to be considered failing
      comparison    = "COMPARISON_LT"
      threshold_value = "0.1"
      trigger {
        count = 1
      }
      aggregations {
        alignment_period = "60s" // 5 min aggregation duration, at least 60s
        //        cross_series_reducer = "REDUCE_SUM"
        per_series_aligner = "ALIGN_RATE"
      }
    }
  }
}

resource "google_monitoring_alert_policy" "job_name_regex_no_reducer_any_series_policy" {
  depends_on = [time_sleep.wait_180_seconds]
  project = var.project
  display_name = "${var.job_base_name}.* 3m job name no reducer any series policy"
  enabled = true
  combiner = "OR"
  notification_channels = [google_monitoring_notification_channel.email.name]
  documentation {
    content = "${var.job_base_name}.* 3m job name no reducer any series documentation"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "${var.job_base_name}.* 3m job name no reducer any series condition"
    condition_threshold {
      filter        = "metric.type=\"dataflow.googleapis.com/job/elements_produced_count\" resource.type=\"dataflow_job\" resource.label.\"job_name\"=monitoring.regex.full_match(\"${var.job_base_name}.*\") metric.label.\"pcollection\"=\"PubsubIO.Read/MapElements/Map.out0\""
      duration      = "180s" // 3 min time that a time series must violate the threshold to be considered failing
      comparison    = "COMPARISON_LT"
      threshold_value = "0.1"
      trigger {
        count = 1
      }
      aggregations {
        alignment_period = "60s" // 5 min aggregation duration, at least 60s
        //        cross_series_reducer = "REDUCE_SUM"
        per_series_aligner = "ALIGN_RATE"
      }
    }
  }
}

resource "google_monitoring_alert_policy" "job_name_sum_reducer_all_series_policy" {
  depends_on = [time_sleep.wait_180_seconds]
  project = var.project
  display_name = "${var.job_name} 3m job name sum reducer all series policy"
  enabled = true
  combiner = "OR"
  notification_channels = [google_monitoring_notification_channel.email.name]
  documentation {
    content = "${var.job_name} 3m job name sum reducer all series documentation"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "${var.job_name} 3m job name sum reducer all series condition"
    condition_threshold {
      filter        = "metric.type=\"dataflow.googleapis.com/job/elements_produced_count\" resource.type=\"dataflow_job\" resource.label.\"job_name\"=\"${var.job_name}\" metric.label.\"pcollection\"=\"PubsubIO.Read/MapElements/Map.out0\""
      duration      = "180s" // 3 min time that a time series must violate the threshold to be considered failing
      comparison    = "COMPARISON_LT"
      threshold_value = "0.1"
      trigger {
        percent = 100.0
      }
      aggregations {
        alignment_period = "60s" // 5 min aggregation duration, at least 60s
        cross_series_reducer = "REDUCE_SUM"
        per_series_aligner = "ALIGN_RATE"
      }
    }
  }
}

resource "google_monitoring_alert_policy" "job_name_regex_sum_reducer_all_series_policy" {
  depends_on = [time_sleep.wait_180_seconds]
  project = var.project
  display_name = "${var.job_base_name}.* 3m job name sum reducer all series policy"
  enabled = true
  combiner = "OR"
  notification_channels = [google_monitoring_notification_channel.email.name]
  documentation {
    content = "${var.job_base_name}.* 3m job name sum reducer all series documentation"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "${var.job_base_name}.* 3m job name sum reducer all series condition"
    condition_threshold {
      filter        = "metric.type=\"dataflow.googleapis.com/job/elements_produced_count\" resource.type=\"dataflow_job\" resource.label.\"job_name\"=monitoring.regex.full_match(\"${var.job_base_name}.*\") metric.label.\"pcollection\"=\"PubsubIO.Read/MapElements/Map.out0\""
      duration      = "180s" // 3 min time that a time series must violate the threshold to be considered failing
      comparison    = "COMPARISON_LT"
      threshold_value = "0.1"
      trigger {
        percent = 100.0
      }
      aggregations {
        alignment_period = "60s" // 5 min aggregation duration, at least 60s
        cross_series_reducer = "REDUCE_SUM"
        per_series_aligner = "ALIGN_RATE"
      }
    }
  }
}

resource "google_monitoring_alert_policy" "job_name_sum_reducer_any_series_policy" {
  depends_on = [time_sleep.wait_180_seconds]
  project = var.project
  display_name = "${var.job_name} 3m job name sum reducer any series policy"
  enabled = true
  combiner = "OR"
  notification_channels = [google_monitoring_notification_channel.email.name]
  documentation {
    content = "${var.job_name} 3m job name sum reducer any series documentation"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "${var.job_name} 3m job name sum reducer any series condition"
    condition_threshold {
      filter        = "metric.type=\"dataflow.googleapis.com/job/elements_produced_count\" resource.type=\"dataflow_job\" resource.label.\"job_name\"=\"${var.job_name}\" metric.label.\"pcollection\"=\"PubsubIO.Read/MapElements/Map.out0\""
      duration      = "180s" // 3 min time that a time series must violate the threshold to be considered failing
      comparison    = "COMPARISON_LT"
      threshold_value = "0.1"
      trigger {
        count = 1
      }
      aggregations {
        alignment_period = "60s" // 5 min aggregation duration, at least 60s
        cross_series_reducer = "REDUCE_SUM"
        per_series_aligner = "ALIGN_RATE"
      }
    }
  }
}

resource "google_monitoring_alert_policy" "job_name_regex_sum_reducer_any_series_policy" {
  depends_on = [time_sleep.wait_180_seconds]
  project = var.project
  display_name = "${var.job_base_name}.* 3m job name sum reducer any series policy"
  enabled = true
  combiner = "OR"
  notification_channels = [google_monitoring_notification_channel.email.name]
  documentation {
    content = "${var.job_base_name}.* 3m job name sum reducer any series documentation"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "${var.job_base_name}.* 3m job name sum reducer any series condition"
    condition_threshold {
      filter        = "metric.type=\"dataflow.googleapis.com/job/elements_produced_count\" resource.type=\"dataflow_job\" resource.label.\"job_name\"=monitoring.regex.full_match(\"${var.job_base_name}.*\") metric.label.\"pcollection\"=\"PubsubIO.Read/MapElements/Map.out0\""
      duration      = "180s" // 3 min time that a time series must violate the threshold to be considered failing
      comparison    = "COMPARISON_LT"
      threshold_value = "0.1"
      trigger {
        count = 1
      }
      aggregations {
        alignment_period = "60s" // 5 min aggregation duration, at least 60s
        cross_series_reducer = "REDUCE_SUM"
        per_series_aligner = "ALIGN_RATE"
      }
    }
  }
}

resource "google_monitoring_alert_policy" "job_id_policy" {
  depends_on = [time_sleep.wait_180_seconds]
  project = var.project
  display_name = "${var.job_name} last run (id=${var.job_id}) did not run for last 3m alert policy"
  enabled = true
  combiner = "OR"
  notification_channels = [google_monitoring_notification_channel.email.name]
  documentation {
    content = "${var.job_name} last run (id=${var.job_id}) did not run for last 3m documentation"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "${var.job_name} last run (id=${var.job_id}) did not run for last 3m condition"
    condition_threshold {
      filter        = "metric.type=\"dataflow.googleapis.com/job/elements_produced_count\" resource.type=\"dataflow_job\" metric.label.\"job_id\"=\"${var.job_id}\" metric.label.\"pcollection\"=\"BigQueryIO.Write/BatchLoads/rewindowIntoGlobal/Window.Assign.out0\""
      duration      = "180s"
      comparison    = "COMPARISON_LT"
      threshold_value = "0.1"
      trigger {
        count = 1
      }
      aggregations {
        alignment_period = "60s"
        per_series_aligner = "ALIGN_RATE"
      }
    }
  }
}
