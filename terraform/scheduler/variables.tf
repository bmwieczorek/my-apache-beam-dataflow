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

variable "network" {
  description = "Google Cloud subnetwork"
  type        = string
  default     = null
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

variable "expiration_date" {
  description = "Expiration date parameter"
  type        = string
  default     = "2021-03-03"
}
