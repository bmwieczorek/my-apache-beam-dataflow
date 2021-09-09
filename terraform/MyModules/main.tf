module "bucket1" {
  source = "./bucket"
  project = var.project
  bucket-name = "my-bucket--1"
}
module "bucket2" {
  source = "./bucket"
  project = var.project
  bucket-name = "my-bucket--2"
}
module "upload-script" {
  source = "./script"
  project = var.project
  bucket-name = module.bucket1.bucket-name
}
