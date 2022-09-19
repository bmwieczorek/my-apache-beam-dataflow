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

variable "job_name" {
  description = "Google dataflow job name (lowercase)"
  type        = string
}

variable "subscription" {
  description = "Google pubsub subscription"
  type        = string
}

variable "max_workers" {
  description = "Dataflow max number of workers"
  type        = number
}

variable "template_gcs_path" {
  description = "Google dataflow template gcs path"
  type        = string
}

variable "table_spec" {
  description = "BigQuery table spec"
  type        = string
}

variable "number_of_worker_harness_threads" {
  description = "Number of harness threads in Dataflow worker"
  type        = number
}

variable "enable_streaming_engine" {
  description = "Whether if streaming engine enabled"
  type        =  bool
}

variable "dump_heap_on_oom" {
  description = "Whether if to create heap dump on OOM"
  type        = bool
}

variable "experiments" {
  description = "Google dataflow job experiments"
  type = list(string)
}

variable "dataflow_classic_template_enabled" {
  description = "If dataflow classic template enabled, otherwise flex template enabled"
  type        = bool
}

variable "owner" {
  description = "Google resource owner"
  type        = string
}
