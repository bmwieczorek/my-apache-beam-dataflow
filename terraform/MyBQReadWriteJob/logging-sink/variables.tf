variable "project" {
  description = "Google Cloud project id"
  type        = string
}

variable "owner" {
  description = "Google resource owner"
  type        = string
}

variable "bucket" {
  description = "Google Cloud bucket for router logs"
  type        = string
}

variable "dataset" {
  description = "Google BigQuery dataset for router logs"
  type        = string
}

variable "job" {
  description = "Google dataflow job name (lowercase)"
  type        = string
}

variable "log_message_pattern" {
  description = "Message pattern to search in logs"
  type        = string
}

variable "dataflow_start_time" {
  description = "Google dataflow job start time"
  type        = string
}
