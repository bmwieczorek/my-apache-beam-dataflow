locals {
  instance          = "${var.job}-vm"
  template_gcs_path = "gs://${var.bucket}/templates/${var.job}-template"
  dataflow_jar     = basename(var.dataflow_jar_local_path)
  startup_script_local_path = "${path.module}/startup-script.sh"
}

resource "google_storage_bucket_object" "dataflow_jar" {
  name   = "compute/${local.dataflow_jar}"
  source = var.dataflow_jar_local_path
  bucket = var.bucket
}

resource "google_storage_bucket_object" "startup_script" {
  name   = "compute/startup-script.sh"
  source = local.startup_script_local_path
  bucket = var.bucket
}

resource "google_compute_instance" "dataflow_classic_template_compute" {
  project      = var.project
  name         = local.instance
  machine_type = "e2-medium"
  zone         = var.zone
  tags = ["default-uscentral1"]
  metadata = {
    "enable-oslogin" = "TRUE"
    "startup-script-url" = "gs://${var.bucket}/${google_storage_bucket_object.startup_script.name}"
    "project" = var.project
    "zone" = var.zone
    "region" = var.region
    "service_account" = var.service_account
    "owner" = var.owner
    "bucket" = var.bucket
    "instance" = local.instance
    "dataflow_jar" = local.dataflow_jar
    "dataflow_jar_gcs_path" = "gs://${var.bucket}/${google_storage_bucket_object.dataflow_jar.name}"
    "template_gcs_path" = local.template_gcs_path
    "dataflow_jar_main_class" = var.main_class
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
    //network = "default" // sandbox: default network, no subnetwork; dev: network null, subnetwork vpc
    network    = var.network
    subnetwork = var.subnetwork == "default" ? null : var.subnetwork
  }

//  metadata_startup_script = "echo hi > /tmp/test.txt"

  service_account {
    # Google recommends custom service accounts that have cloud-platform scope and permissions granted via IAM Roles.
    email  = var.service_account
    scopes = ["cloud-platform"]
  }

  provisioner "local-exec" {
    command = <<EOT
      max_retry=40; counter=1; until gsutil stat ${local.template_gcs_path} ; do sleep 5; if [ $counter -eq $max_retry ]; then echo "Failed" && break; fi; if gcloud compute instances get-serial-port-output ${local.instance} --zone ${var.zone} --project ${var.project} | grep startup | grep script | grep Caused |grep Error ; then echo "java failed" && break; fi; echo "Waiting for template to be generated: $counter attempt" ; counter=$(expr $counter + 1); done
    EOT
  }
}

