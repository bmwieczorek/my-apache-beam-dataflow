resource "google_compute_instance" "vm" {
  name         = "${var.owner}-vm"
  project      = var.project
  zone         = var.zone
  machine_type = "e2-medium"
  metadata = {
    enable-oslogin      = "TRUE"
    amt-idm-hostprofile = var.amt_idm_hostprofile
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

  metadata_startup_script = <<-EOF
    gcloud compute instances add-metadata --zone ${var.zone} ${var.owner}-vm --metadata=startup-state="(1/5) Waiting for internal setup complete ..."

    echo "Checking for internal setup complete ..."
    until ls /etc/*/*.complete1; do
        echo "Waiting for internal setup complete ..."
        sleep 5
    done

    gcloud compute instances add-metadata --zone ${var.zone} ${var.owner}-vm --metadata=startup-state="(2/5) Updating PasswordAuthentication ..."
    mount -o remount,exec /tmp
    sed -i 's/PasswordAuthentication no/PasswordAuthentication yes/g' /etc/ssh/sshd_config
    systemctl restart sshd

    gcloud compute instances add-metadata --zone ${var.zone} ${var.owner}-vm --metadata=startup-state="(3/5) Adding user ..."
    useradd -m -d /home/bartek ${var.owner}
    echo "${var.owner}" | passwd --stdin ${var.owner}

//    ls -la /etc/sudoers
//    sudo cat /etc/sudoers
//    sudo chmod +w /etc/sudoers
//    sudo bash -c 'sudo echo "" > /etc/sudoers'
//    sudo bash -c 'echo "bartek ALL=(ALL:ALL) ALL" >> /etc/sudoers'
//    sudo chmod -w /etc/sudoers
//    ls -la /etc/sudoers
//    sudo cat /etc/sudoers

    gcloud compute instances add-metadata --zone ${var.zone} ${var.owner}-vm --metadata=startup-state="(4/5) Installing Java ..."

    #sudo yum-config-manager --enable rhui-rhel*
    #python -m SimpleHTTPServer 8080

    max_retry=10
    counter=1
    until which java
    do sleep $((counter*10))
      [[ counter -eq $max_retry ]] && echo "Java OpenJDK installation status: failed" &&  gcloud compute instances add-metadata --zone ${var.zone} ${var.owner}-vm --metadata=startup-state="(2/2) Java OpenJDK installation status: failed" && exit 1
      echo "Trying to install java-11-openjdk-devel: $counter attempt"
      sudo yum install java-11-openjdk-devel -y 2>&1
      ((counter++))
    done

    which java
    java -version
    echo "Java OpenJDK installation status: completed"
    gcloud compute instances add-metadata --zone ${var.zone} ${var.owner}-vm --metadata=startup-state="(5/5) Java OpenJDK installation status: completed"
  EOF

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
        counter=$(expr $counter + 1);
      fi
      done
    EOT
  }
}
