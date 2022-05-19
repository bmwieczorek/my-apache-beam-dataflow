variable "project" {
  description = "Google Cloud project id"
  type        = string
}

variable "owner" {
  description = "Google resource owner"
  type        = string
}

variable "job_name_base" {
  description = "Google dataflow job base"
  type        = string
}

variable "job_name" {
  description = "Google dataflow job name (lowercase)"
  type        = string
}

variable "job_id" {
  description = "Google dataflow job id"
  type        = string
}

variable "notification_email" {
  description = "Notification email"
  type        = string
}

//variable "notification_emails" {
//  description = "Notification email"
//  type        = list(string)
//  default = ["a@example.com", "b@example.com"]
//}

// workaround to wait for job to be created

variable "module_depends_on" {
  type        = any
  description = "(optional) A list of external resources the module depends_on. Default is []."
  default     = []
}
