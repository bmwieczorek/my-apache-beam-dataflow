locals {
  debian_version = "debian-11"
  docker_image_name = "${local.debian_version}-slim-airflow-1.10.15"
#  vm_name = replace("${var.owner}-${local.docker_image_name}",".","-")
  vm_name = replace(local.docker_image_name,".","-")
  labels = {
    owner = var.owner
  }
}

module "gce-advanced-container" {
  source = "terraform-google-modules/container-vm/google"
  version = "~> 2.0"

  container = {
    image = "gcr.io/${var.project}/${var.owner}/${local.docker_image_name}"

    securityContext = {
      privileged : true
    }
    tty : true
    env = [
      {
        name  = "BUCKET"
        value = "${var.project}-${var.owner}"
      }
    ]
  }

  restart_policy = "Never"
}


resource "google_compute_instance" "container-vm" {
  project      = var.project
  name         = local.vm_name
  machine_type = "e2-medium"
  zone         = var.zone

  boot_disk {
    initialize_params {
      image = module.gce-advanced-container.source_image
      size = 30
    }
  }

  network_interface {
    subnetwork = var.subnetwork
  }

#  tags = ["${local.name}-tag"] // network tag
  tags = ["default-uscentral1"]

  metadata = {
    gce-container-declaration = module.gce-advanced-container.metadata_value
    google-logging-enabled    = "true"
    google-monitoring-enabled = "true"
  }

  labels = {
    owner = var.owner
    vm = local.vm_name
    container-vm = module.gce-advanced-container.vm_container_label
  }

  service_account {
    email = var.service_account
    scopes = ["cloud-platform"]
  }

  provisioner "local-exec" {
    command = <<EOT
      sleep 10
      instance_id=$(gcloud compute instances describe ${local.vm_name} --project ${var.project} --zone ${var.zone} --format 'value(ID)')
      echo "instance_id=$instance_id"
      max_retry=10
      counter=1
      until
         gcloud logging read "resource.type=gce_instance AND logName=projects/${var.project}/logs/cos_containers AND resource.labels.instance_id=$instance_id" --project ${var.project} --format="value(jsonPayload.message)" | grep "${var.logs_search_pattern}"
      do sleep 30
      if [ $counter -eq $max_retry ]
      then
        echo "INFO ${var.logs_search_pattern} pattern not found in log for vm with instance id $instance_id"
        break
      else
        echo "Waiting for vm with instance id $instance_id to contain "${var.logs_search_pattern}" in logs (attempt: $counter)"
        counter=$(expr $counter + 1)
      fi
      done
    EOT
  }
}
