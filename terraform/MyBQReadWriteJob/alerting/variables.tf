variable "project" {
  description = "Google Cloud project id"
  type        = string
}

variable "job" {
  description = "Google dataflow job name (lowercase)"
  type        = string
}

variable "notification_email" {
  description = "Notification email"
  type        = string
}

variable "owner" {
  description = "Google resource owner"
  type        = string
}