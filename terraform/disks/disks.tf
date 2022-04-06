locals {
  name                        = var.owner
  zone-a                      = "${var.region}-a"
  zone-f                      = "${var.region}-f"
  machine_type                = "e2-micro"

  // vm 1 in zone a writes to attached persistent regional disk then detach it and attach to vm 2 in zone f
  vm1_name                     = "${var.owner}-vm-from-disk-from-debian-image-with-attached-disks"
  vm2_name                     = "${var.owner}-vm-from-disk-from-other-vm-boot-disk-snapshot"
  labels = {
    "owner" : local.name
  }
}

data "google_compute_image" "debian9" {
  project = "debian-cloud"
  family  = "debian-9"
}

resource "google_pubsub_topic" "topic" {
  project    = var.project
  name       = "${local.name}-topic"
  labels     = local.labels
}
resource "google_pubsub_subscription" "subscription" {
  project = var.project
  name    = "${local.name}-subscription"
  topic   = google_pubsub_topic.topic.name
  labels  = local.labels
}

resource "google_compute_disk" "attached-pd-disk" {
  project = var.project
  name  = "${var.owner}-attached-pd-disk"
  size  = 20
  type  = "pd-standard"
  zone  = local.zone-a
}

resource "google_compute_resource_policy" "disk-snapshot-schedule-policy" {
  project = var.project
  name    = "${var.owner}-disk-snapshot-schedule-policy"
  region  = var.region
  snapshot_schedule_policy {
    schedule {
      hourly_schedule {
        hours_in_cycle = 1 // every 1 hour starting 12:00
        start_time = "12:00"
      }
    }
  }
}

resource "google_compute_disk_resource_policy_attachment" "attached-disk-to-snapshot-association" {
  project = var.project
  name    = google_compute_resource_policy.disk-snapshot-schedule-policy.name
  disk    = google_compute_disk.attached-pd-disk.name
  zone    = local.zone-a
}

resource "google_compute_region_disk" "attached-pd-region-disk" {
  project       = var.project
  name          = "${var.owner}-attached-pd-region-disk"
  type          = "pd-balanced"
  region        = var.region
  replica_zones = [local.zone-a, local.zone-f]
}

resource "google_compute_disk" "disk-from-debian-image" {
  project = var.project
  name  = "${var.owner}-disk-from-snapshot"
  size  = 20
  type  = "pd-standard"
  image = data.google_compute_image.debian9.self_link
  zone  = local.zone-a
}

resource "google_compute_instance" "vm-from-disk-from-debian-image" {
  project      = var.project
  name         = local.vm1_name
  machine_type = "n1-standard-1" // n1-standard-1 is compatible with local ssd
  zone         = local.zone-a

  boot_disk {
//    initialize_params {
////      image = "debian-cloud/debian-9"
//      image = data.google_compute_image.debian9.self_link
//    }
    source = google_compute_disk.disk-from-debian-image.self_link
  }

  // Local SSD disk // Hot-remove of local storage is not supported.
  scratch_disk {
    interface = "SCSI"
  }

  attached_disk {
    source = google_compute_disk.attached-pd-disk.self_link
  }

  attached_disk {
    source = google_compute_region_disk.attached-pd-region-disk.self_link
  }

  network_interface {
    network = "default"
  }

  metadata_startup_script = <<-EOF
    echo "$(hostname) $(date) hello boot" | sudo tee -a /boot.txt
    echo "$(hostname) $(date) Mounting attached disk"
    sudo lsblk
    part=$(sudo lsblk | grep 'part /' | grep -oh 'sd[a-z]1' | cut -c1-3 | uniq)
    echo "part=$part"
    devs=$(sudo lsblk | grep disk | grep -v $part | grep -oh 'sd[a-z]')
    echo "devs=$devs"
    for dev in $devs; do
      sudo mkdir -p /mnt/disks/$dev
      sudo mount -o discard,defaults /dev/$dev /mnt/disks/$dev
      RESULT=$?
      if [ $RESULT -eq 0 ]; then
        echo "Mounted /dev/$dev to /mnt/disks/$dev"
      else
        echo "Formatting /dev/$dev to ext4"
        sudo mkfs.ext4 -m 0 -E lazy_itable_init=0,lazy_journal_init=0,discard /dev/$dev
        sudo mount -o discard,defaults /dev/$dev /mnt/disks/$dev
      fi
      echo "$(hostname) $(date) hello pd $dev" | sudo tee -a /mnt/disks/$dev/$dev.txt
      cat /mnt/disks/$dev/$dev.txt
    done
    df -h
    for dev in $devs; do
      sudo umount /mnt/disks/$dev
    done
    gcloud compute instances detach-disk ${local.vm1_name} --zone=${local.zone-a} --disk=${google_compute_disk.attached-pd-disk.name}
    gcloud compute instances detach-disk ${local.vm1_name} --zone=${local.zone-a} --disk-scope=regional --disk=${google_compute_region_disk.attached-pd-region-disk.name}
    df -h
    gcloud pubsub topics publish ${google_pubsub_topic.topic.name} --message="$(hostname) $(date) ${var.owner} started instance"
  EOF

  metadata = {
    shutdown-script =  <<-EOF
      gcloud pubsub topics publish ${google_pubsub_topic.topic.name} --message="$(hostname) $(date) ${var.owner} shutting instance"
    EOF
  }

  service_account {
    # Google recommends custom service accounts that have cloud-platform scope and permissions granted via IAM Roles.
    email  = var.service_account
    scopes = ["cloud-platform"]
  }
}

resource "time_sleep" "wait_60_seconds" {
  depends_on = [google_compute_instance.vm-from-disk-from-debian-image]
  create_duration = "60s"
}

resource "google_compute_snapshot" "snapshot-from-disk-from-image" {
  project           = var.project
  depends_on        = [ google_compute_instance.vm-from-disk-from-debian-image, time_sleep.wait_60_seconds]
  name              = "${var.owner}-snapshot-from-boot-disk"
  source_disk       = google_compute_disk.disk-from-debian-image.self_link
  zone              =  google_compute_disk.disk-from-debian-image.zone
  storage_locations = [var.region]
}

resource "google_compute_disk" "disk-from-snapshot" {
  project = var.project
  name     = "${var.owner}-disk-from-snapshot"
  size     = 20
  type     = "pd-standard"
  snapshot = google_compute_snapshot.snapshot-from-disk-from-image.self_link
  zone     = local.zone-f
}

resource "google_compute_instance" "vm-from-disk-from-snapshot" {
  project      = var.project
  name         = local.vm2_name
  machine_type = "n1-standard-1" // n1-standard-1 is compatible with local ssd
  zone         = local.zone-f

  boot_disk {
    source = google_compute_disk.disk-from-snapshot.self_link // snapshot has /boot.txt populated by vm-from-disk-from-debian-image-with-attached-disks
  }

  network_interface {
    network = "default"
  }

  attached_disk {
    source = google_compute_region_disk.attached-pd-region-disk.self_link
  }

  metadata_startup_script = <<-EOF
    cat /boot.txt
    echo "$(hostname) $(date) Mounting attached disk"
    sudo lsblk
    part=$(sudo lsblk | grep 'part /' | grep -oh 'sd[a-z]1' | cut -c1-3 | uniq)
    echo "part=$part"
    devs=$(sudo lsblk | grep disk | grep -v $part | grep -oh 'sd[a-z]')
    echo "devs=$devs"
    for dev in $devs; do
      sudo mkdir -p /mnt/disks/$dev
      sudo mount -o discard,defaults /dev/$dev /mnt/disks/$dev
      RESULT=$?
      if [ $RESULT -eq 0 ]; then
        echo "Mounted /dev/$dev to /mnt/disks/$dev"
      else
        echo "Formatting /dev/$dev to ext4"
        sudo mkfs.ext4 -m 0 -E lazy_itable_init=0,lazy_journal_init=0,discard /dev/$dev
        sudo mount -o discard,defaults /dev/$dev /mnt/disks/$dev
      fi
      cat /mnt/disks/$dev/sd*.txt
    done
    df -h
  EOF

  service_account {
    # Google recommends custom service accounts that have cloud-platform scope and permissions granted via IAM Roles.
    email  = var.service_account
    scopes = ["cloud-platform"]
  }
}