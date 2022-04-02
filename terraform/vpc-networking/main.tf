locals {
  name                       = var.owner
  worker_vm                  = "${local.name}-worker-vm"
  curl_vm                    = "${local.name}-curl-vm"

  bucket                     = "${local.name}-spring-boot"
  worker-vm-startup-script   = "worker-vm-startup-script.sh"
  curl-vm-startup-script     = "curl-vm-startup-script.sh"
  machine_type               = "e2-micro"

  worker_vm_service_port     = "8080"

  worker_vm_peer_region      = "us-east1"
  curl_vm_peer_region        = "us-west1"
  worker_vm_peer_zone        = "${local.worker_vm_peer_region}-b"
  curl_vm_peer_zone          = "${local.curl_vm_peer_region}-b"

  worker_vm_peer_subnetwork_ip_cidr_range = "10.0.0.0/24"
  curl_vm_peer_subnetwork_ip_cidr_range   = "192.168.0.0/24"
  load_balancer_ip_cidr_range             = "35.0.0.0/8"

  labels = {
    "owner" : local.name
  }
}

// data  Get a datasource info by its name