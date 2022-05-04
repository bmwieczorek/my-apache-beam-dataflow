locals {
  bucket = "${var.project}-${var.owner}-bucket-with-startup-script"
  instance = "${var.owner}-compute-vm"
  dns_vm_instance_name = "${var.owner}-backend-vm"
  dns_domain           = "${var.owner}-gcp.com"
  dns_vm_full_name     = "${local.dns_vm_instance_name}.${local.dns_domain}"
  labels = {
    owner   = var.owner
  }
}

resource "google_storage_bucket" "bucket_with_startup_scripts" {
  name          = local.bucket
  project       = var.project
}

resource "google_storage_bucket_object" "startup_script" {
  name   = "startup-script.sh"
  source = "./startup-script.sh"
  bucket = google_storage_bucket.bucket_with_startup_scripts.name
}

resource "google_storage_bucket_object" "startup_script_ping" {
  name   = "startup-script-ping.sh"
  source = "./startup-script-ping.sh"
  bucket = google_storage_bucket.bucket_with_startup_scripts.name
}

resource "google_compute_instance" "compute_vm" {
  project      = var.project
  name         = local.instance
  machine_type = "e2-medium"
  zone         = var.zone
  tags = ["default-uscentral1"]
  metadata = {
    "enable-oslogin" = "TRUE"
    "startup-script-url" = "gs://${google_storage_bucket.bucket_with_startup_scripts.name}/${google_storage_bucket_object.startup_script.name}"


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
//    network_ip = google_compute_address.compute_vm_address.address // use static ip address or leave empty for automatically assigned

    // sandbox: default network, no subnetwork; dev: network null, subnetwork vpc
    network    = var.network
    subnetwork = var.subnetwork == "default" ? null : var.subnetwork
  }

  //  metadata_startup_script = "echo hi > /tmp/test.txt"

  service_account {
    # Google recommends custom service accounts that have cloud-platform scope and permissions granted via IAM Roles.
    email  = var.service_account
    scopes = ["cloud-platform"]
  }
}

resource "google_compute_address" "compute_vm_address" {
  name = "${local.instance}-static-ip-label"
  project = var.project
  region = var.region
  address_type = "INTERNAL"
  purpose      = "GCE_ENDPOINT"
  subnetwork = var.subnetwork
}

resource "google_dns_managed_zone" "dns_private_zone" {
  project = var.project
  name        = replace(local.dns_domain, ".", "-")
  dns_name    = "${local.dns_domain}."
  description = "${var.owner} private DNS zone"
  labels = local.labels

  visibility = "private"

  private_visibility_config {
    networks {
      network_url = "projects/${var.project}/global/networks/default"
    }
  }
}

resource "google_dns_record_set" "compute_vm_dns" {
  managed_zone = google_dns_managed_zone.dns_private_zone.name
  name = "${local.dns_vm_instance_name}.${google_dns_managed_zone.dns_private_zone.dns_name}"
  type = "A"
//  rrdatas = [google_compute_address.compute_vm_address.address]
  rrdatas = [google_compute_instance.compute_vm.network_interface.0.network_ip]
  ttl = 300
  project = var.project
}

resource "google_compute_instance" "compute_vm_ping" {
  depends_on = [google_dns_record_set.compute_vm_dns]
  project      = var.project
  name         = "${local.instance}-ping"
  machine_type = "e2-medium"
  zone         = var.zone
  tags = ["default-uscentral1"]
  metadata = {
    "startup-script-url" = "gs://${google_storage_bucket.bucket_with_startup_scripts.name}/${google_storage_bucket_object.startup_script_ping.name}"
//    "compute_vm_ip" = google_compute_address.compute_vm_address.address
    "vm_ip_to_ping" = google_compute_instance.compute_vm.network_interface.0.network_ip
    "dns_vm_full_name_to_ping" = local.dns_vm_full_name
  }
  labels = {
    owner   = var.owner
    dns_vm_full_name_to_ping = replace(local.dns_vm_full_name, ".", "_")
    vm_ip_to_ping = replace(google_compute_instance.compute_vm.network_interface[0].network_ip, ".", "_")
  }

  boot_disk {
    initialize_params {
      image = var.image
      type = "pd-balanced"
      size = "25"
    }
  }

  network_interface {
    // sandbox: default network, no subnetwork; dev: network null, subnetwork vpc
    network    = var.network
    subnetwork = var.subnetwork == "default" ? null : var.subnetwork
  }

  //  metadata_startup_script = "echo hi > /tmp/test.txt"

  service_account {
    # Google recommends custom service accounts that have cloud-platform scope and permissions granted via IAM Roles.
    email  = var.service_account
    scopes = ["cloud-platform"]
  }
}
