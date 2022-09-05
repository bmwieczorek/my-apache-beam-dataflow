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

variable "network" {
  description = "Google Cloud subnetwork"
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
  description = "Google dataflow job name (lowercase)"
  type        = string
}

variable "zone" {
  description = "Google Cloud compute zone"
  type        = string
}

variable "image" {
  description = "Google Cloud compute image"
  type        = string
}


variable "dataflow_jar_local_path" {
  description = "Path to dataflow jar on local file system"
  type        = string
}

variable "main_class" {
  description = "Google dataflow job main class"
  type        = string
}

variable "table_spec" {
  description = "BigQuery table spec"
  type        = string
}

variable "owner" {
  description = "Google resource owner"
  type        = string
}
