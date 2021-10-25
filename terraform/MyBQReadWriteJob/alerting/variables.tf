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

variable "log_message_pattern" {
  description = "Message pattern to search in logs"
  type        = string
  default     = "Created MySubscription"
}

//variable "dataflow_start_time" {
//  description = "Google dataflow job start time"
//  type        = string
//}

variable "notification_email" {
  description = "Notification email"
  type        = string
}
