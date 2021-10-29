variable "project" {
  description = "Google Cloud project id"
  type        = string
}

variable "owner" {
  description = "Google resource owner"
  type        = string
}

variable "job" {
  description = "Google dataflow job name (lowercase)"
  type        = string
}

variable "logs_based_metrics_message_pattern" {
  description = "Message pattern to search in logs"
  type        = string
}

variable "notification_email" {
  description = "Notification email"
  type        = string
}

// workaround to wait for job to be created

variable "module_depends_on" {
  type        = any
  description = "(optional) A list of external resources the module depends_on. Default is []."
  default     = []
}
