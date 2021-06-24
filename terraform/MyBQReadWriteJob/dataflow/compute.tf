resource "google_storage_bucket_object" "my_bucket_object_jar" {
  name   = "compute/${var.dataflow_jar}"
  source = "../../../target/${var.dataflow_jar}"
  bucket = var.bucket
}

resource "google_storage_bucket_object" "my_bucket_object_startup_script" {
  name   = "compute/startup-script.sh"
  source = "startup-script.sh"
  bucket = var.bucket
}

resource "google_compute_instance" "compute_template" {
  depends_on = [google_storage_bucket_object.my_bucket_object_jar,google_storage_bucket_object.my_bucket_object_startup_script]
  project      = var.project
  name         = var.instance
  machine_type = "e2-medium"
  zone         = var.zone
  tags = ["default-uscentral1"]
  metadata = {
    "enable-oslogin" = "TRUE",
    "startup-script-url" = "gs://${var.bucket}/compute/startup-script.sh"
    "project" = var.project
    "zone" = var.zone,
    "region" = var.region,
    "service_account" = var.service_account,
    "subnetwork" = var.subnetwork,
    "bucket" = var.bucket,
    "instance" = var.instance,
    "dataflow_jar" = var.dataflow_jar,
    "dataflow_jar_gcs_path" = "gs://${var.bucket}/compute/${var.dataflow_jar}",
    "template_gcs_path" = "gs://${var.bucket}/templates/${var.job}-template",
    "wait_secs_before_delete" = 300
  }
  labels = {
    owner   = var.owner
  }

  boot_disk {
    initialize_params {
      image = var.image
      type = "pd-balanced"
      size = "25"
    }
  }

  network_interface {
    subnetwork = var.subnetwork
  }

//  metadata_startup_script = "echo hi > /tmp/test.txt"

  service_account {
    # Google recommends custom service accounts that have cloud-platform scope and permissions granted via IAM Roles.
    email  = var.service_account
    scopes = ["cloud-platform"]
  }

  provisioner "local-exec" {
    command = <<EOT
      max_retry=50; counter=1; until gsutil stat gs://${var.bucket}/templates/${var.job}-template ; do sleep 10; [[ counter -eq $max_retry ]] && echo "Failed" && break; echo "Wating for template to be generated: $counter attempt" ; ((counter++)); done
    EOT
  }
}

