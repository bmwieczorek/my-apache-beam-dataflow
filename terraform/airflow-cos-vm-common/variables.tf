variable "project" {
  description = "Google Cloud project id"
  type        = string
}

variable "zone" {
  description = "Google Cloud compute zone"
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

variable "service_account" {
  description = "Google Cloud service account email"
  type        = string
}

variable "owner" {
  description = "Google resource owner"
  type        = string
}

variable "logs_search_pattern" {
  description = "Logs search pattern"
  type        = string
  default     = "dag"
}


