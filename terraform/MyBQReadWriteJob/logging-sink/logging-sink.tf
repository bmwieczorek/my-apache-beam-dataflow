resource "google_logging_project_sink" "dataflow_gcs_logging_sink" {
  project     = var.project
  name        = "${var.owner}-dataflow-gcs-logging-sink"
  destination = "storage.googleapis.com/${var.bucket}"
  filter      = "resource.type=\"dataflow_step\" resource.labels.job_name=~\"${var.job}.*\" (logName=\"projects/${var.project}/logs/dataflow.googleapis.com%2Fjob-message\" OR logName=\"projects/${var.project}/logs/dataflow.googleapis.com%2Flauncher\") timestamp >= \"${var.dataflow_start_time}\"  severity>=INFO textPayload=\"${var.log_message_pattern}\""
  description = "Dataflow logging GCS sink matching '${var.log_message_pattern}' pattern for job_name =~ ${var.job}.*"
  unique_writer_identity = true
}

resource "google_project_iam_binding" "gcs_log_writer" {
  project     = var.project
  role        = "roles/storage.objectCreator"
  members     = [ google_logging_project_sink.dataflow_gcs_logging_sink.writer_identity ]
}

resource "google_logging_project_sink" "dataflow_bq_logging_sink" {
  project     = var.project
  name        = "${var.owner}-dataflow-bq-logging-sink"
  destination = "bigquery.googleapis.com/projects/${var.project}/datasets/${var.dataset}"
  filter      = "resource.type=\"dataflow_step\" resource.labels.job_name=~\"${var.job}.*\" (logName=\"projects/${var.project}/logs/dataflow.googleapis.com%2Fjob-message\" OR logName=\"projects/${var.project}/logs/dataflow.googleapis.com%2Flauncher\") timestamp >= \"${var.dataflow_start_time}\"  severity>=INFO textPayload=\"${var.log_message_pattern}\""
  description = "Dataflow logging BQ sink matching '${var.log_message_pattern}' pattern for job_name =~ ${var.job}.*"
  unique_writer_identity = true
}

resource "google_project_iam_binding" "bq_log_writer" {
  project     = var.project
  role        = "roles/bigquery.dataEditor"
  members     = [
    google_logging_project_sink.dataflow_bq_logging_sink.writer_identity
  ]
}

resource "null_resource" "bq_ls_dataset" {
  triggers = {
    always_run = formatdate("YYYY-MM-DD-hh-mm-ss", timestamp())
  }

  provisioner "local-exec" {
    when    = destroy
    command = "bq ls ${var.dataset}"
  }

  depends_on = [google_project_iam_binding.bq_log_writer]
}

resource "null_resource" "bq_select_table" {
  triggers = {
    always_run = formatdate("YYYY-MM-DD-hh-mm-ss", timestamp())
  }

  provisioner "local-exec" {
    when    = destroy
    command = "bq query --use_legacy_sql=false 'SELECT * FROM `${var.dataset}.dataflow_googleapis_com_job_message_*` WHERE REGEXP_CONTAINS(_TABLE_SUFFIX, \"^[0-9]{8}$\")'"
  }

  depends_on = [null_resource.bq_ls_dataset]
}


#bq head --selected_fields=timestamp,severity,textPayload,logName,resource.type,resource.labels.job_id,resource.labels.job_name,resource.labels.region,labels.dataflow_googleapis_com_log_type bartek_mybqreadwritejob.dataflow_googleapis_com_job_message_20230501
#+------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+---------------------+----------+-----------------------------------------------+
#|                                      logName                                       |                                                                           resource                                                                            |     textPayload      |      timestamp      | severity |                    labels                     |
#+------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+---------------------+----------+-----------------------------------------------+
#| projects/my-project.../logs/dataflow.googleapis.com%2Fjob-message | {"type":"dataflow_step","labels":{"region":"us-central1","job_name":"bartek-mybqreadwritejob-2021-03-03","job_id":"2023-05-01_09_14_09-304..."}} | Worker pool stopped. | 2023-05-01 16:17:38 | INFO     | {"dataflow_googleapis_com_log_type":"system"} |
#+------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+---------------------+----------+-----------------------------------------------+