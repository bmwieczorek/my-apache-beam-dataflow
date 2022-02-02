output "dataflow_start_time" {
  value = local.dataflow_start_time
}

output "job_name" {
  value =  (length(google_dataflow_flex_template_job.job) > 0 ? google_dataflow_flex_template_job.job[0].name : null)
}

output "job_id" {
  value = (length(google_dataflow_flex_template_job.job) > 0 ? google_dataflow_flex_template_job.job[0].id : null)
}