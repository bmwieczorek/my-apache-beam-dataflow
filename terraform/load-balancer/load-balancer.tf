locals {
  name                        = var.owner
  bucket                      = "${local.name}-spring-boot"
  worker-vm-startup-script    = "worker-vm-startup-script.sh"
  curl-vm-startup-script      = "curl-vm-startup-script.sh"
  machine_type                = "e2-micro"

  worker_vm_service_port      = "8080"
  lb_service_port             = "80"
  regional_mig_zones          = ["${var.region}-a","${var.region}-b"]
  regional_failover_mig_zones = ["${var.region}-c","${var.region}-f"]

  worker_vm_subnet_ip_cidr_range = "10.0.0.0/24"
  curl_vm_subnet_ip_cidr_range   = "10.0.1.0/24"
  proxy_subnet_ip_cidr_range  = "10.0.2.0/24"
  lb_ip                       = "10.0.1.11"

  dns_vm_instance_name        = "${local.name}-lb"
  dns_domain                  = "${local.name}-gcp.com"
  dns_full_name               = "${local.dns_vm_instance_name}.${local.dns_domain}"

  http_proxy_load_balancer_multiple_backends_autoscaled_mig = 0
  internal_tcp_load_balancer_static_mig = local.http_proxy_load_balancer_multiple_backends_autoscaled_mig == 1 ? 0 : 1

  labels = {
    "owner" : local.name
  }
}

// data  Get a datasource info by its name

resource "google_compute_network" "network" {
  project                 = var.project
  name                    = "${local.name}-network"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "workers_network" {
  project       = var.project
  name          = "${local.name}-workers-network"
  ip_cidr_range = local.worker_vm_subnet_ip_cidr_range
  region        = var.region
  network       = google_compute_network.network.name
  private_ip_google_access = true // keep vm IP internal but allow access public IP for Google services/API e.g. GCS bucket
}

resource "google_compute_subnetwork" "lb-network" {
  project       = var.project
  name          = "${local.name}-lb-network"
  ip_cidr_range = local.curl_vm_subnet_ip_cidr_range
  region        = var.region
  network       = google_compute_network.network.name
  private_ip_google_access = true
}

resource "google_compute_subnetwork" "proxy_subnet" {
  count         =  local.http_proxy_load_balancer_multiple_backends_autoscaled_mig
  project       = var.project
  name          = "${local.name}-l7-ilb-proxy-subnet"
  provider      = google-beta
  ip_cidr_range = local.proxy_subnet_ip_cidr_range
  region        = var.region
  purpose       = "INTERNAL_HTTPS_LOAD_BALANCER"
  role          = "ACTIVE"
  network       = google_compute_network.network.name
}

resource "google_compute_firewall" "health-check-firewall" {
  project       = var.project
  name          = "${local.name}-health-check-firewall"
  network       = google_compute_network.network.name

  allow {
    protocol = "tcp"
    ports    = [local.worker_vm_service_port]
  }

  source_ranges = ["130.211.0.0/22", "35.191.0.0/16"]
}

resource "google_compute_firewall" "ssh-firewall" {
  project       = var.project
  name          = "${local.name}-ssh-firewall"
  network       = google_compute_network.network.name

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}

resource "google_compute_firewall" "internal-traffic-firewall" {
  project       = var.project
  name          = "${local.name}-internal-traffic-firewall"
  network       = google_compute_network.network.name

  allow {
    protocol = "icmp"
  }

  allow {
    protocol = "tcp"
    ports    = [local.lb_service_port,local.worker_vm_service_port]
  }

  source_ranges = [local.worker_vm_subnet_ip_cidr_range,local.curl_vm_subnet_ip_cidr_range,local.proxy_subnet_ip_cidr_range]
}

resource "google_storage_bucket_object" "worker-vm-startup-scripts-object" {
  name   = local.worker-vm-startup-script
  source = "./${local.worker-vm-startup-script}"
  bucket = local.bucket
}

resource "google_compute_instance_template" "template" {
  project        = var.project
  region         = var.region
  name           = "${local.name}-instance-template"
  machine_type   = local.machine_type
  can_ip_forward = false

  tags = ["default-uscentral1"]
  metadata = {
    "enable-oslogin"     = "TRUE"
    "startup-script-url" = "gs://${google_storage_bucket_object.worker-vm-startup-scripts-object.bucket}/${google_storage_bucket_object.worker-vm-startup-scripts-object.name}"
  }

  //metadata_startup_script = "echo Hello"

  disk {
    source_image = var.image
  }

  network_interface {
    subnetwork = google_compute_subnetwork.workers_network.self_link
  }

  service_account {
    scopes = ["cloud-platform"]
  }
}

resource "google_compute_health_check" "health-check" {
  project             = var.project
  name                = "${local.name}-tcp-health-check"
  timeout_sec         = 5
  check_interval_sec  = 10
  healthy_threshold   = 3 # 10 seconds
  unhealthy_threshold = 6 # 60 seconds

  tcp_health_check {
    port = local.worker_vm_service_port
  }
}


resource "google_compute_region_instance_group_manager" "static-mig" {
  count                     = local.internal_tcp_load_balancer_static_mig
  project                   = var.project
  name                      = "${local.name}-static-mig"
  region                    = var.region

  //  target_pools       = [google_compute_target_pool.target_pool.self_link]

  base_instance_name        = "${local.name}-region-worker-vm"
  distribution_policy_zones = local.regional_mig_zones

  version {
    instance_template = google_compute_instance_template.template.self_link
  }

  target_size  = 4 // number of running instances for this mig, do not set if using autoscaler

  // named_port cannot be used when backend service has INTERNAL load balancing

  auto_healing_policies {
    health_check = google_compute_health_check.health-check.id
    initial_delay_sec = 1800
  }
}

resource "google_compute_region_instance_group_manager" "static-failover-mig" {
  count                     = local.internal_tcp_load_balancer_static_mig
  project                   = var.project
  name                      = "${local.name}-static-failover-mig"
  region                    = var.region

  base_instance_name        = "${local.name}-region-worker-vm"
  distribution_policy_zones = local.regional_failover_mig_zones

  version {
    instance_template = google_compute_instance_template.template.self_link
  }

  target_size  = 2 // number of running instances for this mig, do not set if using autoscaler

  // named_port cannot be used when backend service has INTERNAL load balancing

  auto_healing_policies {
    health_check = google_compute_health_check.health-check.id
    initial_delay_sec = 1800
  }
}

resource "google_compute_region_instance_group_manager" "autoscaled-mig" {
  count                     = local.http_proxy_load_balancer_multiple_backends_autoscaled_mig
  project                   = var.project
  name                      = "${local.name}-autoscaled-mig"
  region                    = var.region

  base_instance_name        = "${local.name}-region-worker-vm"
  distribution_policy_zones = local.regional_mig_zones

  version {
    instance_template = google_compute_instance_template.template.self_link
  }

  named_port {
    name = "custom-http"
    port = local.worker_vm_service_port
  }

  auto_healing_policies {
    health_check = google_compute_health_check.health-check.id
    initial_delay_sec = 1800
  }
}

resource "google_compute_region_autoscaler" "autoscaler" {
  count   = local.http_proxy_load_balancer_multiple_backends_autoscaled_mig
  project = var.project
  name    = "${local.name}-autoscaler"
  region  = var.region
  target  = google_compute_region_instance_group_manager.autoscaled-mig[0].self_link

  autoscaling_policy {
    max_replicas    = 3
    min_replicas    = 1
    cooldown_period = 60

    cpu_utilization {
      target = 0.01
    }
  }
}

resource "google_compute_region_health_check" "health-check" {
  project             = var.project
  name                = "${local.name}-tcp-region-health-check"
  region              = var.region
  timeout_sec         = 5
  check_interval_sec  = 10
  healthy_threshold   = 3 # 10 seconds
  unhealthy_threshold = 6 # 60 seconds

  tcp_health_check {
    port = local.worker_vm_service_port
  }
}

resource "google_compute_region_backend_service" "http_backend_service" {
  count                 = local.http_proxy_load_balancer_multiple_backends_autoscaled_mig
  project               = var.project
  name                  = "${local.name}-http-backend"
  region                = var.region
  protocol              = "HTTP"
  load_balancing_scheme = "INTERNAL_MANAGED"
  locality_lb_policy    = "ROUND_ROBIN"

  backend {
    group = google_compute_region_instance_group_manager.autoscaled-mig[0].instance_group
    balancing_mode = "UTILIZATION"
    capacity_scaler = 1.0 // used for INTERNAL_MANAGED load_balancing_scheme
  }
  port_name = "custom-http"
  health_checks = [google_compute_region_health_check.health-check.id]
}

resource "google_compute_region_backend_service" "tcp_backend_service" {
  count                 = local.internal_tcp_load_balancer_static_mig
  project               = var.project
  name                  = "${local.name}-tcp-backend"
  region                = var.region
  protocol              = "TCP"
  load_balancing_scheme = "INTERNAL" // cannot use port_name with INTERNAL
  backend {
    group = google_compute_region_instance_group_manager.static-mig[0].instance_group
    balancing_mode = "CONNECTION"
  }
  backend {
    group = google_compute_region_instance_group_manager.static-failover-mig[0].instance_group
    balancing_mode = "CONNECTION"
  }
  health_checks = [google_compute_region_health_check.health-check.id]
}

resource "google_compute_address" "lb-static-ip-10-0-1-11" {
  project      = var.project
  name         = "${local.name}-lb-static-ip-10-0-1-11"
  region       = var.region
  subnetwork   = google_compute_subnetwork.lb-network.id
  address_type = "INTERNAL"
  address      = local.lb_ip
}

resource "google_compute_region_url_map" "url_map" {
  count           = local.http_proxy_load_balancer_multiple_backends_autoscaled_mig
  project         = var.project
  name            = "${local.name}-l7-ilb-regional-url-map"
  region          = var.region
  default_service = google_compute_region_backend_service.http_backend_service[0].id
}

resource "google_compute_region_target_http_proxy" "http_proxy" {
  count    = local.http_proxy_load_balancer_multiple_backends_autoscaled_mig
  project  = var.project
  name     = "${local.name}-l7-ilb-target-http-proxy"
  region   = var.region
  url_map  = google_compute_region_url_map.url_map[0].id
}

resource "google_compute_forwarding_rule" "http_proxy_backed_forwarding_rule" {
  count                 = local.http_proxy_load_balancer_multiple_backends_autoscaled_mig
  project               = var.project
  name                  = "${local.name}-l4-ilb-forwarding-rule"
  region                = var.region
  ip_address            = google_compute_address.lb-static-ip-10-0-1-11.address
  port_range            = local.lb_service_port
  network               = google_compute_network.network.name
  subnetwork            = google_compute_subnetwork.lb-network.name

  ip_protocol           = "TCP"
  load_balancing_scheme = "INTERNAL_MANAGED"
  depends_on            = [google_compute_subnetwork.proxy_subnet]
  target                = google_compute_region_target_http_proxy.http_proxy[0].id
}


resource "google_compute_forwarding_rule" "tcp_internal_backend_forwarding_rule" {
  count                 = local.internal_tcp_load_balancer_static_mig
  project               = var.project
  name                  = "${local.name}-backend-forwarding-rule"
  region                = var.region
  ip_address            = google_compute_address.lb-static-ip-10-0-1-11.address
  network               = google_compute_network.network.name
  subnetwork            = google_compute_subnetwork.lb-network.name

  ip_protocol           = "TCP"
  backend_service       = google_compute_region_backend_service.tcp_backend_service[0].id // cannot use port_range for backend service based forwarding rules
  load_balancing_scheme = "INTERNAL"
  ports                 = [local.worker_vm_service_port] // just load-balance, not proxy to port local.lb_service_port
}

resource "google_dns_managed_zone" "dns_zone" {
  project     = var.project
  name        = replace(local.dns_domain, ".", "-")
  dns_name    = "${local.dns_domain}."
  description = "${local.name}-private DNS zone"
  labels      = local.labels

  visibility = "private"

  private_visibility_config {
    networks {
      network_url = google_compute_network.network.id
    }
  }
}

resource "google_dns_record_set" "dns_record" {
  managed_zone = google_dns_managed_zone.dns_zone.name
  name    = "${local.dns_vm_instance_name}.${google_dns_managed_zone.dns_zone.dns_name}"
  type    = "A"
  rrdatas = [google_compute_address.lb-static-ip-10-0-1-11.address]
  ttl     = 300
  project = var.project
}

resource "google_storage_bucket_object" "curl_vm_startup_script_object" {
  name   = local.curl-vm-startup-script
  source = "./${local.curl-vm-startup-script}"
  bucket = local.bucket
}

resource "google_compute_instance" "curl_vm" {
  depends_on = [google_dns_record_set.dns_record, google_compute_instance_template.template]
  project      = var.project
  name         = "${local.name}-curl-vm"
  machine_type = local.machine_type
  zone         = var.zone
  tags         = ["default-uscentral1"]
  metadata = {
    "enable-oslogin"     = "TRUE"
    "startup-script-url" = "gs://${local.bucket}/${google_storage_bucket_object.curl_vm_startup_script_object.name}"
    "lb_ip"              = google_compute_address.lb-static-ip-10-0-1-11.address
    "lb_port"            = local.http_proxy_load_balancer_multiple_backends_autoscaled_mig == 1 ? local.lb_service_port : local.worker_vm_service_port
    "lb_dns_full_name"   = local.dns_full_name
  }
  labels = {
    owner            = var.owner
    ip_to_curl       = replace(google_compute_address.lb-static-ip-10-0-1-11.address, ".", "_")
    dns_name_to_curl = replace(local.dns_full_name,".","_")
  }

  boot_disk {
    initialize_params {
      image = var.image
      type  = "pd-balanced"
      size  = "25"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.lb-network.self_link
  }

  //  metadata_startup_script = "echo hi > /tmp/test.txt"

  service_account {
    # Google recommends custom service accounts that have cloud-platform scope and permissions granted via IAM Roles.
    email  = var.service_account
    scopes = ["cloud-platform"]
  }
}

//resource "google_compute_http_health_check" "health-check" {
//  project             = var.project
//  name                = "${local.name}-http-health-check"
//  request_path        = "/"
//  port                = local.worker_vm_service_port
//  timeout_sec         = 5
//  check_interval_sec  = 10
//  healthy_threshold   = 3 # consecutive successes count for healthy status
//  unhealthy_threshold = 6 # consecutive failures count for unhealthy status
//}

//resource "google_compute_target_pool" "target_pool" {
//  project       = var.project
//  name          = "${local.name}-compute-target-pool"
//  region        = var.region
//  health_checks = [google_compute_http_health_check.health-check.name]
//}

//resource "google_compute_forwarding_rule" "target_pool_forwarding_rule" {
//  project               = var.project
//  region                = var.region
//  name                  = "${local.name}-target-pool-forwarding-rule"
//  ip_address            = google_compute_address.lb-static-ip-10-0-1-11.address
//  target                = google_compute_target_pool.target_pool.id
//  ip_protocol           = "TCP"
//  load_balancing_scheme = "INTERNAL"
//  port_range            = local.lb_service_port
//}
