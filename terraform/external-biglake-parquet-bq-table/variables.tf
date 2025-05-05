variable "project" {
  description = "Google Cloud project id"
  type        = string
}

variable "owner" {
  description = "Google resource owner"
  type        = string
}

variable "bq_project" {
  description = "Google BigQuery project id"
  type        = string
}

variable "location" {
    description = "Google resources location"
    type        = string
    default     = "US"
}