resource "google_storage_bucket" "bucket" {
  project	    = var.project
  name          = var.bucket
  force_destroy = true
  labels = {
    owner   = var.owner
  }
}
