resource "google_storage_bucket" "my_bucket" {
  name          = local.bucket
  project       = var.project
  location      = var.region
  storage_class = "REGIONAL"
  force_destroy = true
  labels        = local.labels
}

# to be enabled if storage errors on dataflow logs upon draining
# resource "google_storage_bucket_iam_member" "grant_sa_object_admin_role_to_bucket" {
#   project = var.project
#   bucket  = google_storage_bucket.my_bucket.name
#   role    = "roles/storage.objectAdmin"
#   member  = "serviceAccount:${var.service_account}"
# }
