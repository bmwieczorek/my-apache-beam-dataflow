output "dataflow_start_time" {
  value = local.dataflow_start_time
}

output "job_name" {
  value = (length(google_dataflow_job.job) > 0 ? google_dataflow_job.job[0].name : null)
//  value = join("", google_dataflow_job.job.*.name)
}

output "job_id" {
  value = (length(google_dataflow_job.job) > 0 ? google_dataflow_job.job[0].id : null)
//  value = join("", google_dataflow_job.job.*.id)
}
