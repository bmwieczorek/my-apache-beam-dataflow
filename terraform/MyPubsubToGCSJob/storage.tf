resource "google_storage_bucket" "my_bucket" {
  name          = local.bucket
  project	    = var.project
  location      = var.region
  storage_class = "REGIONAL"
  force_destroy = true
  labels        = local.labels
}
