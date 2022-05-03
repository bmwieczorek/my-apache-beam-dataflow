locals {
  bucket = "${var.owner}-spring-boot"
  startup-script = "startup-script.sh"
  labels = {
    owner = var.owner
  }
}

resource "google_storage_bucket_object" "startup-scripts-bucket-object" {
  name   = local.startup-script
  source = "./${local.startup-script}"
  bucket = local.bucket
}

// use google_compute_address if 1 instance
resource "google_compute_address" "compute-address" {
  name         = "${var.owner}-compute-address"
  project      = var.project
  region       = var.region
  address_type = "INTERNAL"
  purpose      = "GCE_ENDPOINT"
}


resource "google_compute_disk" "compute-disk" {
  name    = "${var.owner}-compute-disk"
  project = var.project
  type    = "pd-ssd"
  zone    = var.zone
  size    = 30
  labels  = local.labels
  physical_block_size_bytes = 4096
}

resource "google_compute_instance_template" "compute-instance-template" {
  name        = "${var.owner}-compute-instance-template"
  description = "${var.owner}-compute-instance-template is used to create compute instances."
  project     = var.project
  tags        = ["default-uscentral1"]

  instance_description = "description assigned to instances"
  machine_type = "e2-medium"
  region       = var.region
  labels       = local.labels
  metadata = {
    "enable-oslogin" = "TRUE"
    "startup-script-url" = "gs://${google_storage_bucket_object.startup-scripts-bucket-object.bucket}/${google_storage_bucket_object.startup-scripts-bucket-object.name}"
    "project" = var.project
    "owner" = var.owner
  }

  scheduling {
    automatic_restart   = true
    on_host_maintenance = "MIGRATE"
  }

  // Create a new boot disk from an image
  disk {
    source_image      = var.image
    auto_delete       = true // use true if 1 instance
//    auto_delete       = false
    boot              = true
//    mode =            "READ_ONLY"
    disk_size_gb      = 30
  }

  // Use an existing disk resource
  disk {
    // Instance Templates reference disks by name, not self link
    source      = google_compute_disk.compute-disk.name
    auto_delete = false
    boot        = false
  }

  lifecycle {
    create_before_destroy = true
  }

  network_interface {
    network_ip = google_compute_address.compute-address.address // use google_compute_address if 1 instance
    network    = var.network
    subnetwork = var.subnetwork == "default" ? null : var.subnetwork
  }

  service_account {
    # Google recommends custom service accounts that have cloud-platform scope and permissions granted via IAM Roles.
    email  = var.service_account
    scopes = ["cloud-platform"]
  }
}

resource "google_compute_health_check" "tcp-8080-compute-health-check" {
  project             = var.project
  name                = "${var.owner}-tcp-8080-compute-health-check"
  timeout_sec         = 5
  check_interval_sec  = 10
  healthy_threshold   = 3 # 10 seconds
  unhealthy_threshold = 6 # 60 seconds

  tcp_health_check {
    port = "8080"
  }
}

resource "google_compute_instance_group_manager" "compute-instance-group-manager" {
  project            = var.project
  name               = "${var.owner}-compute-instance-group-manager"
  wait_for_instances = true
  //  wait_for_instances_status = local.instance_autoheal.wait_for_instances_status
  base_instance_name = "${var.owner}-mig-compute-vm"
  zone = var.zone
  version {
    instance_template = google_compute_instance_template.compute-instance-template.id
  }
  target_size = 1
  auto_healing_policies {
    health_check = google_compute_health_check.tcp-8080-compute-health-check.id
    initial_delay_sec = 1800
  }
}