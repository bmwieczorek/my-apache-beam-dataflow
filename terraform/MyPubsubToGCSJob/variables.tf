variable "project" {
  description = "Google Cloud project id"
  type        = string
}

variable "bucket" {
  description = "Google Cloud bucket"
  type        = string
}

variable "topic" {
  description = "Google Pub/Sub topic"
  type        = string
}

variable "subscription" {
  description = "Google Pub/Sub subscription"
  type        = string
}

variable "job" {
  description = "Google dataflow job name (lowercase)"
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

variable "output" {
  description = "Google GCS output"
  type        = string
}

variable "dashboard_file" {
  description = "Google dashboard file name"
  type        = string
}

variable "owner" {
  description = "Google resource owner"
  type        = string
}
