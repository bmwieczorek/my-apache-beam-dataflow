resource "google_monitoring_dashboard" "dashboard_job_id" {
  project = var.project
  dashboard_json = templatefile(var.dashboard_file, {
    job = var.job
    dataflow_job_filter = "metric.label.\\\"job_id\\\"=\\\"${google_dataflow_flex_template_job.my_dataflow_flex_job.id}\\\""
    instance_dataflow_job_filter = "metadata.user_labels.\\\"dataflow_job_id\\\"=\\\"${google_dataflow_flex_template_job.my_dataflow_flex_job.id}\\\""
    dashboard_name = "${var.job} last run by job id"
    topic = var.topic
    subscription = var.subscription
  })
}

resource "google_monitoring_dashboard" "dashboard_job_name" {
  project = var.project
  dashboard_json = templatefile(var.dashboard_file, {
    job = var.job
    dataflow_job_filter = "resource.label.\\\"job_name\\\"=\\\"${var.job}\\\""
    instance_dataflow_job_filter = "metadata.user_labels.\\\"dataflow_job_name\\\"=\\\"${var.job}\\\""
    dashboard_name = "${var.job} all runs by job name"
    topic = var.topic
    subscription = var.subscription
  })
}
