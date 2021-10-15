resource "google_compute_instance" "vm" {
  name         = "${var.owner}-vm"
  project      = var.project
  zone         = var.zone
  machine_type = "e2-medium"
  metadata = {
    "enable-oslogin" = "TRUE"
  }

  boot_disk {
    initialize_params {
      image = var.image
    }
  }

  network_interface {
    network    = var.network
    subnetwork = var.subnetwork
  }

  metadata_startup_script = <<-EOF
    echo "Hello ${var.owner}-vm!"
  EOF

  service_account {
    scopes = ["compute-ro", "storage-ro"]
  }
}
