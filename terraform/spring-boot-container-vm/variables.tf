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
//  default     = null
}

variable "subnetwork" {
  description = "Google Cloud subnetwork"
  type        = string
}

variable "owner" {
  description = "Google resource owner"
  type        = string
}
