resource "google_compute_instance" "vm" {
  name         = "${var.owner}-vm"
  project      = var.project
  zone         = var.zone
  machine_type = "e2-medium"
  tags = ["default-uscentral1"]
  metadata = {
    "enable-oslogin" = "TRUE"
  }
  labels = {
    owner   = var.owner
  }

  boot_disk {
    initialize_params {
      image = var.image
      type = "pd-balanced"
      size = "25"
    }
  }

  network_interface {
    network    = var.network
    subnetwork = var.subnetwork
  }

  metadata_startup_script = "hello!"

  service_account {
    scopes = ["compute-ro", "storage-ro"]
  }
}
