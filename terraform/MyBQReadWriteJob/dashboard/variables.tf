variable "project" {
  description = "Google Cloud project id"
  type        = string
}

variable "dashboard_file" {
  description = "Google dashboard file name"
  type        = string
}

variable "job" {
  description = "Google dataflow job name (lowercase)"
  type        = string
}

variable "owner" {
  description = "Google resource owner"
  type        = string
}
