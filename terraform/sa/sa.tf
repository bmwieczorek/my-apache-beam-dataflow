locals {
  bucket              = "${var.project}-${var.owner}-bucket-with-startup-script"
  startup-script-name = "startup-script.sh"
}

resource "google_service_account" "sa" {
  project      = var.project
  account_id   = "${var.owner}-sa"
  display_name = "${var.owner} main sa with storage.objectViewer at project level"
//  display_name = "${var.owner}-sa@bartek-project-123.iam.gserviceaccount.com"
}

resource "google_project_iam_member" "sa_roles" {
  project     = var.project
  role        = "roles/storage.objectViewer"
  member      = "serviceAccount:${google_service_account.sa.email}"
}

resource "google_storage_bucket" "bucket" {
  location      = "US"
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
    email  = google_service_account.sa.email
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

resource "google_service_account" "sa_admin_project" {
  project      = var.project
  account_id   = "${var.owner}-sa-admin-project"
  display_name = "${var.owner} sa with iam.serviceAccountAdmin at project level"
//  display_name = "${var.owner}-sa-admin-project@bartek-project-123.iam.gserviceaccount.com"
}

resource "google_project_iam_member" "sa_admin_role" {
  project     = var.project
  role        = "roles/iam.serviceAccountAdmin"
  member      = "serviceAccount:${google_service_account.sa_admin_project.email}"
}

resource "google_service_account" "sa_user_project" {
  project      = var.project
  account_id   = "${var.owner}-sa-user-project"
  display_name = "${var.owner} sa with iam.serviceAccountUser at project level"
//  display_name = "${var.owner}-sa-user-project@bartek-project-123.iam.gserviceaccount.com"
}

resource "google_project_iam_member" "sa_user_role" {
  project     = var.project
  role        = "roles/iam.serviceAccountUser"
  member      = "serviceAccount:${google_service_account.sa_user_project.email}"
}

resource "google_service_account" "sa_admin_sa" {
  project      = var.project
  account_id   = "${var.owner}-sa-admin-sa"
  display_name = "${var.owner} sa with iam.serviceAccountAdmin for Bartek main sa"
//  display_name = "${var.owner}-sa-admin-sa@bartek-project-123.iam.gserviceaccount.com"
}

resource "google_service_account_iam_member" "sa_admin_sa_role" {
  service_account_id = google_service_account.sa.name
  role               = "roles/iam.serviceAccountAdmin"
  member             = "serviceAccount:${google_service_account.sa_admin_sa.email}"
}

resource "google_service_account" "sa_user_sa" {
  project      = var.project
  account_id   = "${var.owner}-sa-user-sa"
  display_name = "${var.owner} sa with iam.serviceAccountUser for Bartek main sa"
//  display_name = "${var.owner}-sa-user-sa@bartek-project-123.iam.gserviceaccount.com"
}

resource "google_service_account_iam_member" "sa_user_sa_role" {
  service_account_id = google_service_account.sa.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.sa_user_sa.email}"
}