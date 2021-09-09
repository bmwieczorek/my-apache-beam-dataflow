resource "google_storage_bucket" "my-bucket" {
  name   = var.bucket-name
  project = var.project
}