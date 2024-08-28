variable "project" {
  description = "Google Cloud project id"
  type        = string
}

variable "bucket" {
  description = "Google Cloud bucket"
  type        = string
}

variable "dataset" {
  description = "Google BQ dataset"
  type        = string
}

variable "table" {
  description = "Google BQ table"
  type        = string
  default     = "mysubscription_table"
}

variable "view" {
  description = "Google BQ view"
  type        = string
  default     = "mysubscription_view"
}

variable "table_schema_file" {
  description = "File with BigQuery table schema in avro format"
  type        = string
}

variable "load_file" {
  description = "My load file"
  type        = string
  default     = "mysubscription_table.json"
}

variable "owner" {
  description = "Google resource owner"
  type        = string
}

variable "service_account" {
  description = "Google Cloud service account email"
  type        = string
}
