locals {
  name = "${var.owner}-cos-vm"
  labels = {
    owner = var.owner
  }
  debian_version = "debian-11"
}

module "gce-advanced-container" {
  source = "terraform-google-modules/container-vm/google"

  container = {
    image = "gcr.io/${var.project}/${var.owner}/debian-11-testing"

    securityContext = {
      privileged : true
    }
    tty : true
    env = [
      {
        name  = "MYENV"
        value = "XYZ"
      },
      {
        name  = "MY_DOCKER_RUN_COMMAND_LINE_ENV"
        value = "AABBCC"
      }
    ]
  }

  restart_policy = "Never"
}


resource "google_compute_instance" "container-vm" {
  project      = var.project
  name         = local.name
  machine_type = "e2-micro"
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

  tags = ["${local.name}-tag"] // network tag

  metadata = {
    gce-container-declaration = module.gce-advanced-container.metadata_value
    google-logging-enabled    = "true"
    google-monitoring-enabled = "true"
  }

  labels = {
    owner = var.owner
    vm = local.name
    container-vm = module.gce-advanced-container.vm_container_label
  }

  service_account {
    email = var.service_account
    scopes = ["cloud-platform"]
  }

  provisioner "local-exec" {
    command = <<EOT
      sleep 10
      instance_id=$(gcloud compute instances describe ${local.name} --project ${var.project}  --zone ${var.zone}  --format 'value(ID)')
      echo "instance_id=$instance_id"
      max_retry=10
      counter=1
      until
         gcloud logging read "resource.type=gce_instance AND logName=projects/${var.project}/logs/cos_containers AND resource.labels.instance_id=$instance_id" --project ${var.project} --format="value(jsonPayload.message)" | grep '/entrypoint.sh'
      do sleep 30
      if [ $counter -eq $max_retry ]
      then
        echo "INFO '/entrypoint.sh' pattern not found in log for vm with instance id $instance_id"
        break
      else
        echo "Waiting for vm with instance id $instance_id to contain '/entrypoint.sh' in logs (attempt: $counter)"
        counter=$(expr $counter + 1)
      fi
      done
    EOT
  }
}
