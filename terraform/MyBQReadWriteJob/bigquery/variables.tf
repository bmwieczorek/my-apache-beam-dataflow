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
}

variable "view" {
  description = "Google BQ view"
  type        = string
}

variable "table_schema_file" {
  default = "File with BigQuery table schema in avro format"
  type        = string
}

variable "load_file" {
  description = "My load file"
  type = string
}

variable "owner" {
  description = "Google resource owner"
  type        = string
}
