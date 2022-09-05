variable "project" {
  description = "Google Cloud project id"
  type        = string
}

variable "region" {
  description = "Google Cloud region"
  type        = string
}

variable "zone" {
  description = "Google Cloud compute zone"
  type        = string
}

variable "service_account" {
  description = "Google Cloud service account email"
  type        = string
}

variable "network" {
  description = "Google Cloud network"
  type        = string
}

variable "subnetwork" {
  description = "Google Cloud subnetwork"
  type        = string
}

variable "image" {
  description = "Google Cloud compute image"
  type        = string
}

variable "dashboard_file" {
  description = "Google dashboard file name"
  type        = string
  default     = "dashboard.json"
}

variable "owner" {
  description = "Google resource owner"
  type        = string
}

variable "dataflow_classic_template_enabled" {
  description = "If dataflow classic template enabled, otherwise flex template enabled"
  type        = bool
  default     = true
}

variable "notification_email" {
  description = "Notification email"
  type        = string
}

variable "force_job_replace" {
  description = "If dataflow name should be dynamically re-calculated forcing job draining/canceling instead of updating"
  type        = bool
  default     = true
}
