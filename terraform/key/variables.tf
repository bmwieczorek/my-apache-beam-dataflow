variable "project" {
  description = "Google Cloud project id"
  type        = string
}

variable "region" {
  description = "Google Cloud region"
  type        = string
}

variable "provision_keyring" {
  description = "Provision KMS Key Ring: true or false"
  type        = bool
  default     = true
}

variable "provision_key" {
  description = "Provision Key : true or false"
  type        = bool
  default     = true
}

variable "owner" {
  description = "Google resource owner"
  type        = string
}