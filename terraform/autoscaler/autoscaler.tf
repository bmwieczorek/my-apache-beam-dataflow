locals {
  labels = {
    "owner" : var.owner
  }
}

resource "google_compute_autoscaler" "compute_autoscaler" {
  name   = "${var.owner}-compute-autoscaler"
  project = var.project
  zone = var.zone
  target = google_compute_instance_group_manager.compute_instance_group_manager.self_link
//  target = google_compute_region_instance_group_manager.compute_region_instance_group_manager.self_link

  autoscaling_policy {
    max_replicas    = 3
    min_replicas    = 1
    cooldown_period = 60

    cpu_utilization {
      target = 0.01
    }
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

resource "google_compute_instance_template" "compute_instance_template" {
  project = var.project
  region = var.region
  name           = "${var.owner}-compute-instance-template"
  machine_type   = "e2-medium"
  can_ip_forward = false

  tags = ["default-uscentral1"]
  metadata = {
    "enable-oslogin" = "TRUE"
  }

  metadata_startup_script = <<-EOF
    LOG="/tmp/startup-script.txt"

    echo "Checking java version" | tee -a $LOG
    java -version 2>&1 | tee -a $LOG

    echo "Copying spring boot from gsc" | tee -a $LOG
    gsutil cp gs://${var.project}-${var.owner}/spring-boot-0.0.1-SNAPSHOT.jar . 2>&1 | tee -a $LOG

    echo "Starting java spring boot" | tee -a $LOG
    nohup java -jar spring-boot-0.0.1-SNAPSHOT.jar >> $LOG 2>&1&
    tail -f $LOG
  EOF

  disk {
    source_image = data.google_compute_image.eda-common.self_link
//    source_image = data.google_compute_image.debian_11.self_link
  }

  network_interface {
    network = "default"
  }

  service_account {
    scopes = ["cloud-platform"]
  }
}

resource "google_compute_target_pool" "compute_target_pool" {
  project = var.project
  region = var.region
  name = "${var.owner}-compute-target-pool"
}

// zonal
resource "google_compute_instance_group_manager" "compute_instance_group_manager" {
  name = "${var.owner}-compute-instance-group-manager"
  project = var.project
  zone = var.zone

  version {
    instance_template  = google_compute_instance_template.compute_instance_template.self_link
    name               = "primary"
  }

  target_pools       = [google_compute_target_pool.compute_target_pool.self_link]
  base_instance_name = "${var.owner}-autoscaler-vm"
  auto_healing_policies {
    health_check = google_compute_health_check.tcp-8080-compute-health-check.id
    initial_delay_sec = 1800
  }
}

// regional - multiple zones
//resource "google_compute_region_instance_group_manager" "compute_region_instance_group_manager" {
//  name = "${var.owner}-compute-region-instance-group-manager"
//  project = var.project
//
//  base_instance_name         = "${var.owner}-autoscaler-vm"
//  region                     = "us-central1"
//  distribution_policy_zones  = ["us-central1-a", "us-central1-f"]
//
//  version {
//    instance_template = google_compute_instance_template.compute_instance_template.self_link
//  }
//
//  target_pools = [google_compute_target_pool.compute_target_pool.self_link]
//  target_size  = 6 // number of running instances for this mig, do not set if using autoscaler
////
////  named_port {
////    name = "custom"
////    port = 8888
////  }
//
//  auto_healing_policies {
//    health_check = google_compute_health_check.tcp-8080-compute-health-check.id
//    initial_delay_sec = 1800
//  }
//}


data "google_compute_image" "debian_11" {
  family  = "debian-11"
  project = "debian-cloud"
}

data "google_compute_image" "eda-common" {
  name = "eda-common-vm-0-0-1-20210506"
  project = var.project
}



//resource "google_compute_instance" "debian_vm" {
//  project      = var.project
//  name         = "${var.owner}-debian-vm"
//  machine_type = "e2-medium"
//  zone         = var.zone
//  tags = ["default-uscentral1"]
//  metadata = {
//    "enable-oslogin" = "TRUE"
//  }
//
//  boot_disk {
//    initialize_params {
//      image = "debian-cloud/debian-9"
//    }
//  }
//
//  network_interface {
//    network    = var.network
//    subnetwork = var.subnetwork
//  }
//
//  metadata_startup_script = "echo Hello"
//
//  service_account {
//    email  = var.service_account
//    scopes = ["cloud-platform"]
//  }
//}
//
//

//resource "google_compute_target_http_proxy" "default" {
//  name    = "test-proxy"
//  project = var.project
//  url_map = google_compute_region_url_map.default.id
//}
//
//resource "google_compute_region_url_map" "default" {
//  name            = "url-map"
//  region = var.region
//  project         = var.project
//  default_service = google_compute_region_backend_service.default.id
//}

# HTTP target proxy
resource "google_compute_region_target_http_proxy" "default" {
  project  = var.project
  name     = "l7-ilb-target-http-proxy"
  region   = var.region
  url_map  = google_compute_region_url_map.default.id
}

# URL map
resource "google_compute_region_url_map" "default" {
  project         = var.project
  name            = "l7-ilb-regional-url-map"
  region          = var.region
  default_service = google_compute_region_backend_service.default.id
}

data "google_compute_address" "lb-ip" {
  name = "lb-static-ip-label"
  project = var.project
  region = var.region
}

//resource "google_compute_forwarding_rule" "http" {
//  project    = var.project
//  region = var.region
//
//  name       = "${var.owner}-http-rule"
//  target     = google_compute_target_http_proxy.default.self_link
//  ip_address = data.google_compute_address.lb-ip.address
//  port_range = "80"
//}

//resource "google_compute_region_backend_service" "default" {
//  project     = var.project
//  region = var.region
//  name        = "${var.owner}-backend"
//  protocol    = "TCP"
//  timeout_sec = 10
//  load_balancing_scheme = "INTERNAL"
//  backend {
//    group = google_compute_instance_group_manager.compute_instance_group_manager.instance_group
//  }
//
//  health_checks = [google_compute_health_check.tcp-8080-compute-health-check.id]
//}

resource "google_compute_region_backend_service" "default" {
  project     = var.project
  region = var.region
  name        = "${var.owner}-backend"
  protocol = "HTTP"
  load_balancing_scheme = "INTERNAL_MANAGED"
  locality_lb_policy = "ROUND_ROBIN"
  backend {
    group = google_compute_instance_group_manager.compute_instance_group_manager.instance_group
    balancing_mode = "UTILIZATION"
    capacity_scaler = 1.0
  }

  health_checks = [google_compute_health_check.tcp-8080-compute-health-check.id]
}

resource "google_compute_forwarding_rule" "google_compute_forwarding_rule" {
  project               = var.project
  name                  = "${var.owner}-l4-ilb-forwarding-rule"
  region                = var.region
  port_range            = 80
  network               = var.network
  ip_protocol           = "TCP"
  load_balancing_scheme = "INTERNAL_MANAGED"
  ip_address            = data.google_compute_address.lb-ip.address
  target                = google_compute_region_target_http_proxy.default.id
  subnetwork            = var.subnetwork
  depends_on            = [google_compute_subnetwork.proxy_subnet]

}

resource "google_compute_subnetwork" "proxy_subnet" {
  project       = var.project
  name          = "${var.owner}-l7-ilb-proxy-subnet"
  provider      = google-beta
  ip_cidr_range = "10.1.0.0/26"
  region        = var.region
  purpose       = "INTERNAL_HTTPS_LOAD_BALANCER"
  role          = "ACTIVE"
  network       = var.network
}


//resource "google_compute_forwarding_rule" "default" {
//  project    = var.project
//  region = var.region
//  name       = "website-forwarding-rule"
//  ip_address = data.google_compute_address.lb-ip.address
//  target     = google_compute_target_pool.compute_target_pool.id
//    ip_protocol           = "TCP"
//    load_balancing_scheme = "INTERNAL"
//  port_range = "80"
//}

//resource "google_compute_forwarding_rule" "default" {
//  project    = var.project
//  name   = "website-forwarding-rule"
//  region = var.region
//  ip_protocol           = "TCP"
//  load_balancing_scheme = "INTERNAL_MANAGED"
//  port_range            = "80"
//  target                = google_compute_target_http_proxy.default.id
//  network               = var.network
//}