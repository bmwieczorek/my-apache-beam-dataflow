locals {
  labels = {
    owner = var.owner
  }
  debian_version = "debian-11"
}
resource "google_compute_instance" "vm" {
  name         = "${var.owner}-${local.debian_version}"
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
//    network    = var.network
//    subnetwork = var.subnetwork == "default" ? null : var.subnetwork
    subnetwork = google_compute_subnetwork.subnetwork.self_link
  }

  metadata_startup_script = <<-EOF
        sudo apt-get update -y
        echo "Hello from startup script"
  EOF

  service_account {
    // email  = var.service_account
    // Generally, you can just set the cloud-platform access scope to allow access to most of the Cloud APIs, then grant the service account only relevant IAM roles. The combination of access scopes granted to the virtual machine instance and the IAM roles granted to the service account determines the amount of access the service account has for that instance. The service account can execute API methods only if they are allowed by both the access scope and its IAM roles.
    scopes = ["cloud-platform"]
  }

  provisioner "local-exec" {
    command = <<EOT
      max_retry=10
      counter=1
      until
         gcloud compute instances get-serial-port-output ${google_compute_instance.vm.name} --zone ${var.zone} --project ${var.project} | grep startup | grep script | grep Hello
      do sleep 60
      if [ $counter -eq $max_retry ]
      then
        echo "Startup script pattern not found in ${google_compute_instance.vm.name} logs"
        break
      else
        echo "Waiting for ${google_compute_instance.vm.name} logs to contain startup script (attempt: $counter)"
        counter=$(expr $counter + 1);
      fi
      done
    EOT
  }
}
