locals {
  bucket              = "${var.project}-${var.owner}-bucket-with-startup-script"
  startup-script-name = "startup-script.sh"
}

resource "google_storage_bucket" "bucket" {
  project       = var.project
  name          = local.bucket
}

resource "google_storage_bucket_object" "startup_script" {
  name   = local.startup-script-name
  content = <<-EOF
    echo "Hello from startup script, listing bucket gs://${google_storage_bucket.bucket.name}"
    gsutil ls gs://${google_storage_bucket.bucket.name}
  EOF
  bucket = google_storage_bucket.bucket.name
}

resource "google_compute_instance" "vm" {
  name         = "${var.owner}-vm"
  project      = var.project
  zone         = var.zone
  machine_type = "e2-medium"
  metadata = {
    "enable-oslogin" = "TRUE"
    "startup-script-url" = "gs://${google_storage_bucket.bucket.name}/${google_storage_bucket_object.startup_script.name}"
  }

  boot_disk {
    initialize_params {
      image = var.image
    }
  }

  network_interface {
    network    = var.network
    subnetwork = var.subnetwork
  }

//  metadata_startup_script = <<-EOF
//        echo "Hello from startup script, listing bucket gs://${google_storage_bucket.bucket.name}"
//        gsutil ls gs://${google_storage_bucket.bucket.name}
//  EOF

  service_account {
//    email  = var.service_account
    scopes = ["cloud-platform"]
  }

  provisioner "local-exec" {
    command = <<EOT
      max_retry=3
      counter=1
      until
         gcloud compute instances get-serial-port-output ${google_compute_instance.vm.name} --zone $ZONE --project $PROJECT | grep startup | grep script
      do sleep 40
      if [[ counter -eq $max_retry ]]
      then
        echo "Startup script pattern not found in ${google_compute_instance.vm.name} logs"
        break
      else
        echo "Waiting for ${google_compute_instance.vm.name} logs to contain startup script (attempt: $counter)"
        ((counter++))
      fi
      done
    EOT
  }
}
