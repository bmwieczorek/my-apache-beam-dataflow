output "dataflow_start_time" {
  value = local.dataflow_start_time
}

output "job_name" {
  value = google_dataflow_job.job.name
}

output "job_id" {
  value = google_dataflow_job.job.id
}
