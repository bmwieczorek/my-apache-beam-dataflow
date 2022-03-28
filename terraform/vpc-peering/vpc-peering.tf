locals {
  name                       = var.owner
  bucket                     = "${local.name}-spring-boot"
  worker-vm-startup-script   = "worker-vm-startup-script.sh"
  curl-vm-startup-script     = "curl-vm-startup-script.sh"
  machine_type               = "e2-micro"

  worker_vm_service_port     = "8080"

  worker_vm_peer_region = "us-east1"
  curl_vm_peer_region   = "us-west1"
  worker_vm_peer_zone = "${local.worker_vm_peer_region}-b"
  curl_vm_peer_zone   = "${local.curl_vm_peer_region}-b"
  worker_vm_peer_subnetwork_ip_cidr_range = "10.0.0.0/24"
  curl_vm_peer_subnetwork_ip_cidr_range   = "192.168.0.0/24"

  labels = {
    "owner" : local.name
  }
}

// data  Get a datasource info by its name

resource "google_compute_network" "worker_vm_peer_network" {
  project                 = var.project
  name                    = "${local.name}-worker-vm-peer-network"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "worker_vm_peer_subnetwork" {
  project       = var.project
  name          = "${local.name}-worker-vm-peer-subnetwork"
  ip_cidr_range = local.worker_vm_peer_subnetwork_ip_cidr_range
  region        = local.worker_vm_peer_region
  network       = google_compute_network.worker_vm_peer_network.name
  private_ip_google_access = true
}

resource "google_compute_network" "curl_vm_peer_network" {
  project                 = var.project
  name                    = "${local.name}-curl-vm-peer-network"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "curl_vm_peer_subnetwork" {
  project       = var.project
  name          = "${local.name}-curl-vm-peer-subnetwork"
  ip_cidr_range = local.curl_vm_peer_subnetwork_ip_cidr_range
  region        = local.curl_vm_peer_region
  network       = google_compute_network.curl_vm_peer_network.name
  private_ip_google_access = true
}

resource "google_compute_network_peering" "worker_vm_network_curl_vm_network_peering" {
  name         = "worker-vm-network-curl-vm-network-peering"
  network      = google_compute_network.worker_vm_peer_network.self_link
  peer_network = google_compute_network.curl_vm_peer_network.self_link
}

resource "google_compute_network_peering" "curl_vm_network_worker_vm_network_peering" {
  name         = "curl-vm-network-worker-vm-network-peering"
  network      = google_compute_network.curl_vm_peer_network.self_link
  peer_network = google_compute_network.worker_vm_peer_network.self_link
}

resource "google_compute_firewall" "worker_vm_peer_network_firewall" {
  project       = var.project
  name          = "${local.name}-worker-vm-peer-network-firewall"
  network       = google_compute_network.worker_vm_peer_network.name

  allow {
    protocol = "icmp"
  }

  allow {
    protocol = "tcp"
    ports    = ["22",local.worker_vm_service_port]
  }
}

resource "google_compute_firewall" "curl_vm_peer_network_firewall" {
  project       = var.project
  name          = "${local.name}-curl-vm-peer-network-firewall"
  network       = google_compute_network.curl_vm_peer_network.name

  allow {
    protocol = "icmp"
  }

  allow {
    protocol = "tcp"
    ports    = ["22",local.worker_vm_service_port]
  }
}

resource "google_storage_bucket_object" "worker-vm-startup-scripts-object" {
  name   = local.worker-vm-startup-script
  source = "./${local.worker-vm-startup-script}"
  bucket = local.bucket
}

resource "google_compute_instance" "worker_vm" {
  project = var.project
  name = "${local.name}-worker-vm"
  zone = local.worker_vm_peer_zone
  machine_type = local.machine_type
  metadata = {
    "enable-oslogin" = "TRUE"
    "startup-script-url" = "gs://${local.bucket}/${google_storage_bucket_object.worker-vm-startup-scripts-object.name}"
  }
  labels = local.labels
  boot_disk {
    initialize_params {
      image = var.image
      type = "pd-balanced"
      size = "25"
    }
  }
  network_interface {
    subnetwork = google_compute_subnetwork.worker_vm_peer_subnetwork.self_link
  }
  service_account {
    # Google recommends custom service accounts that have cloud-platform scope and permissions granted via IAM Roles.
    email = var.service_account
    scopes = ["cloud-platform"]
  }
  allow_stopping_for_update = true
}

resource "google_compute_instance_group" "instance_group" {
  project   = var.project
  name      = "${local.name}-instance-group"
  zone      = local.worker_vm_peer_zone
  instances = [google_compute_instance.worker_vm.id]
  named_port {
    name = "http"
    port = "8080"
  }
  lifecycle {
    create_before_destroy = true
  }
}

resource "google_compute_http_health_check" "health-check" {
  project             = var.project
  name                = "${local.name}-http-health-check"
  request_path        = "/"
  port                = local.worker_vm_service_port
  timeout_sec         = 5
  check_interval_sec  = 10
  healthy_threshold   = 3 # consecutive successes count for healthy status
  unhealthy_threshold = 6 # consecutive failures count for unhealthy status
}

resource "google_compute_backend_service" "backend_service" {
  project   = var.project
  name      = "${local.name}-backend-service"
  port_name = "http"
  protocol  = "HTTP"

  backend {
    group = google_compute_instance_group.instance_group.id
  }

  health_checks = [
    google_compute_http_health_check.health-check.id,
  ]
}


resource "google_storage_bucket_object" "curl_vm_startup_script_object" {
  name   = local.curl-vm-startup-script
  source = "./${local.curl-vm-startup-script}"
  bucket = local.bucket
}

data "google_compute_image" "debian_11" {
  family  = "debian-11"
  project = "debian-cloud"
}

resource "google_compute_instance" "curl_vm" {
  project      = var.project
  name         = "${local.name}-curl-vm"
  machine_type = local.machine_type
  zone         = local.curl_vm_peer_zone
  metadata = {
    "enable-oslogin"     = "TRUE"
    "startup-script-url" = "gs://${local.bucket}/${google_storage_bucket_object.curl_vm_startup_script_object.name}"
    "ip"              = google_compute_instance.worker_vm.network_interface[0].network_ip
    "port"            = local.worker_vm_service_port
  }
  labels = {
    owner            = var.owner
    ip_to_curl       = replace(google_compute_instance.worker_vm.network_interface[0].network_ip,".","_")
  }

  boot_disk {
    initialize_params {
//      image = var.image
      image = data.google_compute_image.debian_11.self_link
      type  = "pd-balanced"
      size  = "25"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.curl_vm_peer_subnetwork.self_link
  }

  service_account {
    # Google recommends custom service accounts that have cloud-platform scope and permissions granted via IAM Roles.
    email  = var.service_account
    scopes = ["cloud-platform"]
  }
}


resource "google_compute_router" "router" {
  project = var.project
  name    = "${local.name}-cloud-router"
  region  = google_compute_subnetwork.curl_vm_peer_subnetwork.region
  network = google_compute_network.curl_vm_peer_network.id

  bgp {
    asn = 64514
  }
}

resource "google_compute_router_nat" "nat" {
  project                            = var.project
  name                               = "${local.name}-cloud-router-nat"
  router                             = google_compute_router.router.name
  region                             = google_compute_router.router.region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"

  log_config {
    enable = true
    filter = "ERRORS_ONLY"
  }
}