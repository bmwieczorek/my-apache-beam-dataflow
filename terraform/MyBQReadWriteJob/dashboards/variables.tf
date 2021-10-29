variable "project" {
  description = "Google Cloud project id"
  type        = string
}

variable "job" {
  description = "Google dataflow job name (lowercase)"
  type        = string
}

variable "dataflow_job_id" {
  description = "Google dataflow job id"
  type        = string
}

variable "dashboard_file" {
  description = "Google dashboard file name"
  type        = string
  default     = "dashboard.json"
}

variable "logs_based_metric_type" {
  description = "Google logs based metric type"
  type        = string
}
