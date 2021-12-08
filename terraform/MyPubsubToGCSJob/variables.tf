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

variable "subnetwork" {
  description = "Google Cloud subnetwork"
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
