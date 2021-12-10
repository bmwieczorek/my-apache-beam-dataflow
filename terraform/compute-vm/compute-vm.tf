locals {
  bucket              = "${var.project}-${var.owner}-vm-startup-script"
  startup-script-name = "startup-script.sh"
}

resource "google_storage_bucket" "bucket" {
  project       = var.project
  name          = local.bucket
}


resource "google_storage_bucket_object" "startup_script" {
  name   = local.startup-script-name
  content = <<-EOF
    # No supported authentication methods available (server sent: publickey,gssapi-keyex,gssapi-with-mic)
    gcloud compute instances add-metadata --zone ${var.zone} ${var.owner}-vm --metadata=startup-state="(1/3) Updating PasswordAuthentication ..."
    sed -i 's/PasswordAuthentication no/PasswordAuthentication yes/g' /etc/ssh/sshd_config
    systemctl restart sshd

    gcloud compute instances add-metadata --zone ${var.zone} ${var.owner}-vm --metadata=startup-state="(2/3) Checking Java ..."

    max_retry=10
    counter=1
    until which java
    do sleep $((counter*10))
      [[ counter -eq $max_retry ]] && echo "Java OpenJDK installation status: failed" &&  gcloud compute instances add-metadata --zone ${var.zone} ${var.owner}-vm --metadata=startup-state="(3/3) Java OpenJDK installation status: failed" && exit 1
      echo "Trying to install java-11-openjdk-devel: $counter attempt"
      sudo yum install java-11-openjdk-devel -y 2>&1
      ((counter++))
    done

    which java
    java -version
    echo "Java OpenJDK installation status: completed"
    gcloud compute instances add-metadata --zone ${var.zone} ${var.owner}-vm --metadata=startup-state="(3/3) Java OpenJDK installation status: completed"
  EOF
  bucket = google_storage_bucket.bucket.name
}

resource "google_compute_instance" "vm" {
  name         = "${var.owner}-vm"
  project      = var.project
  zone         = var.zone
  machine_type = "e2-medium"
  metadata = {
    enable-oslogin      = "TRUE"
    amt-idm-hostprofile = var.amt_idm_hostprofile
    startup-script-url  = "gs://${google_storage_bucket.bucket.name}/${google_storage_bucket_object.startup_script.name}"
  }

  boot_disk {
    initialize_params {
      image = var.image
      size = 128
    }
  }

  network_interface {
    network    = var.network
    subnetwork = var.subnetwork == "default" ? null : var.subnetwork
  }

//  metadata_startup_script = <<-EOF
//        echo "Hello from startup script, listing bucket gs://${google_storage_bucket.bucket.name}"
//        gsutil ls gs://${google_storage_bucket.bucket.name}
//  EOF

  service_account {
    email  = var.service_account
    scopes = ["cloud-platform"]
  }

  provisioner "local-exec" {
    command = <<EOT
      max_retry=10
      counter=1
      until
         gcloud compute instances get-serial-port-output ${google_compute_instance.vm.name} --zone ${var.zone} --project ${var.project} | grep startup | grep script | grep OpenJDK
      do sleep 60
      if [ $counter -eq $max_retry ]
      then
        echo "Startup script pattern not found in ${google_compute_instance.vm.name} logs"
        break
      else
        echo "Waiting for ${google_compute_instance.vm.name} logs to contain startup script (attempt: $counter)"
        counter=$(expr $counter + 1)
      fi
      done
    EOT
  }
}
