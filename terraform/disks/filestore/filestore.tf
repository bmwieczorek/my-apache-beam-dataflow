locals {
  name                        = var.owner
  zone-a                      = "${var.region}-a"
  share                       = "share1"
  labels = {
    "owner" : local.name
  }
}

resource "google_filestore_instance" "filestore" {
  project  = var.project
  name     = "${var.owner}-filestore-instance"
  location = local.zone-a
  tier     = "BASIC_HDD"
  zone     = local.zone-a
  labels   = local.labels

  file_shares {
    capacity_gb = 2560
    name        = local.share
  }

  networks {
    network = google_compute_network.network.name
    reserved_ip_range = "10.0.1.0/29"
    modes   = ["MODE_IPV4"]
  }
}