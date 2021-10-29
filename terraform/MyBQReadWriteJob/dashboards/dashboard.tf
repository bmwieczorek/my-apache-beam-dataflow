resource "google_monitoring_dashboard" "job_name_dashboard" {
  project = var.project
  dashboard_json = templatefile("${path.module}/${var.dashboard_file}", {
    job = var.job
    dataflow_job_filter = "resource.label.\\\"job_name\\\"=monitoring.regex.full_match(\\\"${var.job}.*\\\")"
    instance_dataflow_job_filter = "metadata.user_labels.\\\"dataflow_job_name\\\"=monitoring.regex.full_match(\\\"${var.job}.*\\\")"
    dashboard_name = "${var.job} - all jobs"
    read_step_name = "BigQueryIO.TypedRead"
    read_step_pcollection = "BigQueryIO.TypedRead/ReadFiles.out0"
    transform_step_name = "To GenericRecords"
    transform_step_pcollection = "To GenericRecords/Map.out0"
    write_step_name = "BigQueryIO.Write"
    // BigQueryIO.Write/PrepareWrite/ParDo(Anonymous).out0
    write_step_pcollection = "BigQueryIO.Write/BatchLoads/rewindowIntoGlobal/Window.Assign.out0"
    logs_based_metric_type = var.logs_based_metric_type
  })
}

resource "google_monitoring_dashboard" "job_id_dashboard" {
  project = var.project
  dashboard_json = templatefile("${path.module}/${var.dashboard_file}", {
    job = var.job
    dataflow_job_filter = "metric.label.\\\"job_id\\\"=\\\"${var.dataflow_job_id}\\\""
    instance_dataflow_job_filter = "metadata.user_labels.\\\"dataflow_job_id\\\"=\\\"${var.dataflow_job_id}\\\""
    dashboard_name = "${var.job} - recent job"
    read_step_name = "BigQueryIO.TypedRead"
    read_step_pcollection = "BigQueryIO.TypedRead/ReadFiles.out0"
    transform_step_name = "To GenericRecords"
    transform_step_pcollection = "To GenericRecords/Map.out0"
    write_step_name = "BigQueryIO.Write"
    write_step_pcollection = "BigQueryIO.Write/BatchLoads/rewindowIntoGlobal/Window.Assign.out0"
    logs_based_metric_type = var.logs_based_metric_type
  })
}
