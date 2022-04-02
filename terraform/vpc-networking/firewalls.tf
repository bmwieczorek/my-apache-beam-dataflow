resource "google_compute_firewall" "worker_vm_peer_network_firewall" {
  project       = var.project
  name          = "${local.worker_vm}-peer-network-firewall"
  network       = google_compute_network.worker_vm_peer_network.name

  allow {
    protocol = "icmp"
  }

  allow {
    protocol = "tcp"
    ports    = [local.worker_vm_service_port]
  }

  source_ranges = [local.curl_vm_peer_subnetwork_ip_cidr_range,local.load_balancer_ip_cidr_range]
  enable_logging = true
}

resource "google_compute_firewall" "curl_vm_peer_network_firewall" {
  project       = var.project
  name          = "${local.curl_vm}-peer-network-firewall"
  network       = google_compute_network.curl_vm_peer_network.name

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
  source_ranges = ["0.0.0.0/0"]
}
