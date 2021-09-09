resource "google_storage_bucket_object" "my_bucket_object_script" {
  name   = "my_bucket_object_script"
  source = "./script/script.sh"
  bucket = var.bucket-name
}