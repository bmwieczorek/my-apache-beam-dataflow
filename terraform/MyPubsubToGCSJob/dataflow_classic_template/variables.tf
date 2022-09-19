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

variable "job_name" {
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

variable "dataflow_jar_local_path" {
  description = "Path to dataflow jar on local file system"
  type        = string
}

variable "main_class" {
  description = "Google dataflow job main class"
  type        = string
}

variable "message_deduplication_enabled" {
  description = "If dataflow should deduplicate messages based on message attribute"
  type        = bool
}

variable "custom_event_time_timestamp_attribute_enabled" {
  description = "If dataflow should use custom attribute as timestamp attribute"
  type        = bool
}

variable "owner" {
  description = "Google resource owner"
  type        = string
}

variable "dataflow_classic_template_enabled" {
  description = "If dataflow classic template enabled, otherwise flex template enabled"
  type        = bool
}
