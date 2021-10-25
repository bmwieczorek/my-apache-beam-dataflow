variable "project" {
  description = "Google Cloud project id"
  type        = string
}

variable "owner" {
  description = "Google resource owner"
  type        = string
}

variable "bucket" {
  description = "Google Cloud bucket"
  type        = string
}

variable "job" {
  description = "Google dataflow job name (lowercase)"
  type        = string
}

variable "log_message_pattern" {
  description = "Message pattern to search in logs"
  type        = string
  default     = "Worker pool stopped."
}

variable "dataflow_start_time" {
  description = "Google dataflow job start time"
  type        = string
}
