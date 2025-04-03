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

variable "dataflow_message_deduplication_enabled" {
  description = "If dataflow should deduplicate messages based on message attribute"
  type        = bool
  default     = true
}

variable "dataflow_custom_event_time_timestamp_attribute_enabled" {
  description = "If dataflow should use custom attribute as timestamp attribute"
  type        = bool
  default     = true
}

variable "dataflow_custom_event_time_timestamp_attribute" {
  description = "Custom attribute as timestamp attribute"
  type        = string
  default     = "et"
}

variable "notification_email" {
  description = "Notification email"
  type        = string
}

variable "skip_wait_on_job_termination" {
  type = bool
  default = false
}

variable "recalculate_template" {
  type = bool
  default = true
}


