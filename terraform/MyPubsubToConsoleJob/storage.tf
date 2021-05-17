resource "google_storage_bucket" "my_bucket" {
  name          = var.bucket
  project	      = var.project
  force_destroy = true
  labels = {
    user = var.label
  }
}
