resource "google_storage_bucket" "bucket_with_standard_and_nearline_storage_objects" {
  provider = google-beta
  name          = "${var.project}_${var.owner}_std_and_nrln_strg_clss"
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    action {
      type = "SetStorageClass"
      storage_class = "STANDARD"
    }
    condition {
      matches_prefix = ["landing/"]
    }
  }

  lifecycle_rule {
    action {
      type = "SetStorageClass"
      storage_class = "NEARLINE"
    }
    condition {
      matches_prefix = ["archive/"]
    }
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      matches_prefix = ["archive/"]
      age = 30
    }
  }
}

resource "google_storage_bucket_object" "upload_file_to_standard_landing" {
  name   = "landing/s.txt"
  source = "s.txt"
  bucket = google_storage_bucket.bucket_with_standard_and_nearline_storage_objects.name
}

resource "google_storage_bucket_object" "upload_file_to_nearline_archive" {
  name   = "archive/n.txt"
  source = "n.txt"
  bucket = google_storage_bucket.bucket_with_standard_and_nearline_storage_objects.name
}
