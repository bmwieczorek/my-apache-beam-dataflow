/*
resource "google_monitoring_dashboard" "dashboard_job_id" {
  project = var.project
  dashboard_json = templatefile(var.dashboard_file, {
    job = var.job
    dataflow_job_filter = "metric.label.\\\"job_id\\\"=\\\"${google_dataflow_job.my_dataflow_job.id}\\\""
    instance_dataflow_job_filter = "metadata.user_labels.\\\"dataflow_job_id\\\"=\\\"${google_dataflow_job.my_dataflow_job.id}\\\""
    dashboard_name = "${var.job} last run by job id"
  })
}

resource "google_monitoring_dashboard" "dashboard_job_name" {
  project = var.project
  dashboard_json = templatefile(var.dashboard_file, {
    job = var.job
    dataflow_job_filter = "resource.label.\\\"job_name\\\"=monitoring.regex.full_match(\\\"${var.job}.*\\\")"
    instance_dataflow_job_filter = "metadata.user_labels.\\\"dataflow_job_name\\\"=monitoring.regex.full_match(\\\"${var.job}.*\\\")"
    dashboard_name = "${var.job} all runs by job name"
  })
}
*/

resource "google_monitoring_dashboard" "dashboard_job_name" {
  project = var.project
  dashboard_json = templatefile("dashboard_batch_redesigned.json", {
    job = var.job
    dataflow_job_filter = "resource.label.\\\"job_name\\\"=monitoring.regex.full_match(\\\"${var.job}.*\\\")"
    instance_dataflow_job_filter = "metadata.user_labels.\\\"dataflow_job_name\\\"=monitoring.regex.full_match(\\\"${var.job}.*\\\")"
    dashboard_name = "${var.job} redesigned job name"
    read_step_name = "BigQueryIO.TypedRead"
    read_step_pcollection = "BigQueryIO.TypedRead/ReadFiles.out0"
    transform_step_name = "To GenericRecords"
    transform_step_pcollection = "To GenericRecords/Map.out0"
    write_step_name = "BigQueryIO.Write"
    // BigQueryIO.Write/PrepareWrite/ParDo(Anonymous).out0
    write_step_pcollection = "BigQueryIO.Write/BatchLoads/rewindowIntoGlobal/Window.Assign.out0"
  })
}

resource "google_monitoring_dashboard" "dashboard_job_id" {
  project = var.project
  dashboard_json = templatefile("dashboard_batch_redesigned.json", {
    job = var.job
    dataflow_job_filter = "metric.label.\\\"job_id\\\"=\\\"${google_dataflow_job.my_dataflow_job.id}\\\""
    instance_dataflow_job_filter = "metadata.user_labels.\\\"dataflow_job_id\\\"=\\\"${google_dataflow_job.my_dataflow_job.id}\\\""
    dashboard_name = "${var.job} redesigned job id"
    read_step_name = "BigQueryIO.TypedRead"
    read_step_pcollection = "BigQueryIO.TypedRead/ReadFiles.out0"
    transform_step_name = "To GenericRecords"
    transform_step_pcollection = "To GenericRecords/Map.out0"
    write_step_name = "BigQueryIO.Write"
    write_step_pcollection = "BigQueryIO.Write/BatchLoads/rewindowIntoGlobal/Window.Assign.out0"
  })
}
