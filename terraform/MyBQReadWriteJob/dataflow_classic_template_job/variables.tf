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

variable "owner" {
  description = "Google resource owner"
  type        = string
}

variable "service_account" {
  description = "Google Cloud service account email"
  type        = string
}

variable "network" {
  description = "Google Cloud network"
  type        = string
  default     = null
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
  description = "Google dataflow job name prefix (lowercase)"
  type        = string
}

variable "expiration_date" {
  description = "Dataflow expiration date param in yyyy-MM-dd format"
  type        = string
}

variable "template_gcs_path" {
  description = "Google dataflow template gcs path"
  type        = string
}
