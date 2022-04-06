resource "google_compute_instance" "vm" {
  project      = var.project
  name         = "${var.owner}-vm"
  machine_type = "n1-standard-1" // n1-standard-1 is compatible with local ssd
  zone         = local.zone-a

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-9"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.subnetwork.self_link
  }

  metadata_startup_script = <<-EOF
    sudo apt-get -y update && sudo apt-get install nfs-common
    sudo mkdir -p /mnt/filestore
    sudo mount 10.0.1.2:/${local.share}  /mnt/filestore
    sudo chmod -R 777 /mnt/filestore
    echo "filestore" | tee -a /mnt/filestore/filestore.txt
    #filestore
    cat /mnt/filestore/filestore.txt
    #filestore
    df -h --type=nfs
    #Filesystem        Size  Used Avail Use% Mounted on
    #10.0.1.2:/share1  2.5T     0  2.4T   0% /mnt/filestore
  EOF

  //  metadata = {
  //    "enable-oslogin" = "TRUE"
  //    shutdown-script =  <<-EOF
  //    EOF
  //  }

  service_account {
    # Google recommends custom service accounts that have cloud-platform scope and permissions granted via IAM Roles.
    email  = var.service_account
    scopes = ["cloud-platform"]
  }
}
