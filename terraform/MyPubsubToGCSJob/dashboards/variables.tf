variable "project" {
  description = "Google Cloud project id"
  type        = string
}

variable "dashboard_file" {
  description = "Google dashboard file name"
  type        = string
  default     = "dashboard.json"
}

variable "job_name" {
  description = "Google dataflow job name (lowercase)"
  type        = string
}

variable "job_id" {
  description = "Google dataflow job id"
  type        = string
}

variable "job_base_name" {
  description = "Google dataflow job name base"
  type        = string
}

variable "topic" {
  description = "Google dataflow topic"
  type        = string
}

variable "subscription" {
  description = "Google dataflow subscription"
  type        = string
}
