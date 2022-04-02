resource "google_compute_network" "worker_vm_peer_network" {
  project                 = var.project
  name                    = "${local.worker_vm}-peer-network"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "worker_vm_peer_subnetwork" {
  project       = var.project
  name          = "${local.worker_vm}-peer-subnetwork"
  ip_cidr_range = local.worker_vm_peer_subnetwork_ip_cidr_range
  region        = local.worker_vm_peer_region
  network       = google_compute_network.worker_vm_peer_network.name
  private_ip_google_access = true
}

resource "google_compute_network" "curl_vm_peer_network" {
  project                 = var.project
  name                    = "${local.curl_vm}-peer-network"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "curl_vm_peer_subnetwork" {
  project       = var.project
  name          = "${local.curl_vm}-peer-subnetwork"
  ip_cidr_range = local.curl_vm_peer_subnetwork_ip_cidr_range
  region        = local.curl_vm_peer_region
  network       = google_compute_network.curl_vm_peer_network.name
  private_ip_google_access = true
}

resource "google_compute_network_peering" "network_peering1" {
  name         = "${local.curl_vm}-network-peering1 "
  network      = google_compute_network.worker_vm_peer_network.self_link
  peer_network = google_compute_network.curl_vm_peer_network.self_link
}

resource "google_compute_network_peering" "network_peering2" {
  name         = "${local.curl_vm}-network-peering2"
  network      = google_compute_network.curl_vm_peer_network.self_link
  peer_network = google_compute_network.worker_vm_peer_network.self_link
}