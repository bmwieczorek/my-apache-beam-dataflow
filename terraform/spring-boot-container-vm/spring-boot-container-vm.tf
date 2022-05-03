locals {
  name = "${var.owner}-spring-boot-container-vm"
  labels = {
    owner = var.owner
  }
  debian_version = "debian-11"
}

module "gce-container" {
  source = "terraform-google-modules/container-vm/google"
  version = "~> 2.0"

  container = {
    image = "gcr.io/${var.project}/${var.owner}/spring-boot:v1"
  }

  restart_policy = "Always"
}


resource "google_compute_instance" "container-vm" {
  project      = var.project
  name         = local.name
  machine_type = "e2-micro"
  zone         = var.zone

  boot_disk {
    initialize_params {
      image = module.gce-container.source_image
    }
  }

  network_interface {
    //    subnetwork = google_compute_subnetwork.subnetwork.self_link
    network    = var.network
    subnetwork = var.subnetwork == "default" ? null : var.subnetwork
  }

  tags = ["${local.name}-tag"]

  metadata = {
    gce-container-declaration = module.gce-container.metadata_value
    google-logging-enabled    = "true"
    google-monitoring-enabled = "true"
  }

  labels = {
    owner = var.owner
    vm = local.name
    container-vm = module.gce-container.vm_container_label
  }

  service_account {
    scopes = ["cloud-platform"]
  }

  provisioner "local-exec" {
    command = <<EOT
      sleep 10
      instance_id=$(gcloud compute instances describe ${local.name}  --zone ${var.zone}  --format 'value(ID)')
      echo "instance_id=$instance_id"
      max_retry=10
      counter=1
      until
         gcloud logging read "resource.type=gce_instance AND logName=projects/${var.project}/logs/cos_containers AND resource.labels.instance_id=$instance_id"  --limit 20 --format="value(jsonPayload.message)" | grep INFO | grep Start
      do sleep 30
      if [ $counter -eq $max_retry ]
      then
        echo "INFO Start pattern not found in log for vm with instance id $instance_id"
        break
      else
        echo "Waiting for vm with instance id $instance_id to contain INFO Start in logs (attempt: $counter)"
        counter=$(expr $counter + 1)
      fi
      done
    EOT
  }
}

resource "google_compute_instance" "ping_vm" {
  name         = "${var.owner}-ping-vm"
  project      = var.project
  zone         = var.zone
  machine_type = "e2-micro"
  metadata = {
//    enable-oslogin      = "TRUE"
//    "startup-script-url" = "gs://..."
    shutdown-script =  <<-EOF
        echo "Bye from shutdown script"
      EOF
  }
  labels = local.labels

  boot_disk {
    initialize_params {
      image = "debian-cloud/${local.debian_version}"
    }
  }

  network_interface {
//    subnetwork = google_compute_subnetwork.subnetwork.self_link
    network    = var.network
    subnetwork = var.subnetwork == "default" ? null : var.subnetwork
  }

  metadata_startup_script = <<-EOF
      max_retry=30
      counter=1
      url="http://${google_compute_instance.container-vm.network_interface[0].network_ip}:8080"
      until
         curl ${url}| grep Greetings
      do sleep 10
      if [ $counter -eq $max_retry ]
      then
        echo "Failed to connect/get response from ${url}"
        break
      else
        echo "Waiting to get reponse from ${url} (attempt: $counter)"
        counter=$(expr $counter + 1);
      fi
      done
  EOF

  service_account {
    // email  = var.service_account
    // Generally, you can just set the cloud-platform access scope to allow access to most of the Cloud APIs, then grant the service account only relevant IAM roles. The combination of access scopes granted to the virtual machine instance and the IAM roles granted to the service account determines the amount of access the service account has for that instance. The service account can execute API methods only if they are allowed by both the access scope and its IAM roles.
    scopes = ["cloud-platform"]
  }

  provisioner "local-exec" {
    command = <<EOT
      max_retry=30
      counter=1
      until
         gcloud compute instances get-serial-port-output ${google_compute_instance.ping_vm.name} --zone ${var.zone} --project ${var.project} | grep startup | grep script | grep Greetings
      do sleep 10
      if [ $counter -eq $max_retry ]
      then
        echo "Startup script pattern not found in ${google_compute_instance.ping_vm.name} logs"
        break
      else
        echo "Waiting for ${google_compute_instance.ping_vm.name} logs to contain startup script (attempt: $counter)"
        counter=$(expr $counter + 1);
      fi
      done
    EOT
  }
}
