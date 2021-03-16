variable "project" {
  description = "Google Cloud project id"
  type        = string
}

variable "region" {
  description = "Google Cloud region"
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

variable "bucket" {
  description = "Google Cloud bucket"
  type        = string
}

variable "job" {
  description = "Google dataflow job name (lowercase)"
  type        = string
}

variable "expiration_date" {
  description = "Dataflow expiration date param in yyyy-MM-dd format"
  type        = string
}

variable "label" {
  description = "Google label"
  type        = string
}
