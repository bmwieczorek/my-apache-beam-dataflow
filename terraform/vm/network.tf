resource "google_compute_network" "network" {
  project                 = var.project
  name                    = "${var.owner}-custom-network"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "subnetwork" {
  project       = var.project
  name          = "${var.owner}-private-ip-google-access-subnetwork"
  ip_cidr_range = "10.0.0.0/24"
  region        = var.region
  network       = google_compute_network.network.name
  private_ip_google_access = true // to access google services e.g.: GCS
}

resource "google_compute_firewall" "allow-ssh-firewall" {
  project       = var.project
  name          = "${var.owner}-allow-ssh-firewall"
  network       = google_compute_network.network.name

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
  source_ranges = ["0.0.0.0/0"]
}