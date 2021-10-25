resource "google_storage_bucket" "my_bucket" {
  project	    = var.project
  name          = var.bucket
  force_destroy = true
  labels = {
    owner   = var.owner
  }
}
