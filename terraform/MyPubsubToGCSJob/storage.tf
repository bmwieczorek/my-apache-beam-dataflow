locals {
  job          = "${var.owner}-mypubsubtogcsjob"
  bucket       =  "${var.project}-${local.job}"
  topic        = "${var.owner}-topic"
  subscription = "${local.topic}-sub"
  labels       = {
    owner = var.owner
  }
}

resource "google_storage_bucket" "my_bucket" {
  name          = local.bucket
  project	    = var.project
  force_destroy = true
  labels        = local.labels
}
