locals {
  instance          = "${var.job_base_name}-vm"
  template_gcs_path = "gs://${var.bucket}/templates/${var.job_base_name}-template"
  dataflow_jar     = basename(var.dataflow_jar_local_path)
  startup_script_local_path = "${path.module}/startup-script.sh"
  subnetwork_name_last_element = split("/", var.subnetwork)[length(split("/", var.subnetwork)) - 1]
}

# good network - use terraform to copy dataflow jar
resource "google_storage_bucket_object" "dataflow_jar" {
 count  = var.dataflow_classic_template_enabled && !var.poor_network_copy_dataflow_jar_via_gsutil ? 1 : 0
 name   = "compute/${local.dataflow_jar}"
 source = var.dataflow_jar_local_path
 bucket = var.bucket
}

# poor network - copy dataflow jar to using gsutil
resource "null_resource" "gsutil_upload_dataflow_jar" {
  count = var.poor_network_copy_dataflow_jar_via_gsutil ? 1 : 0
  triggers = {
    always_run = formatdate("YYYY-MM-DD-hh-mm-ss", timestamp())
  }

  provisioner "local-exec" {
    command = "gsutil -o GSUtil:parallel_composite_upload_threshold=150M cp ${var.dataflow_jar_local_path} gs://${var.bucket}/compute/${local.dataflow_jar}"
  }
}

resource "google_storage_bucket_object" "startup_script" {
  count  = var.dataflow_classic_template_enabled ? 1 : 0
  name   = "compute/startup-script.sh"
  source = local.startup_script_local_path
  bucket = var.bucket
}

resource "google_compute_instance" "dataflow_classic_template_compute" {
  count  = var.dataflow_classic_template_enabled ? 1 : 0
  project      = var.project
  name         = local.instance
  machine_type = "e2-medium"
  zone         = var.zone
  tags = ["default-uscentral1"]
  metadata = {
    "enable-oslogin" = "TRUE"
    "startup-script-url" = "gs://${var.bucket}/${google_storage_bucket_object.startup_script[0].name}"
    "project" = var.project
    "zone" = var.zone
    "region" = var.region
    "service_account" = var.service_account
    "owner" = var.owner
    "bucket" = var.bucket
    "instance" = local.instance
    "dataflow_jar" = local.dataflow_jar
    "dataflow_jar_gcs_path" = var.poor_network_copy_dataflow_jar_via_gsutil ? "gs://${var.bucket}/compute/${local.dataflow_jar}" : "gs://${var.bucket}/${google_storage_bucket_object.dataflow_jar[0].name}"
    "template_gcs_path" = local.template_gcs_path
    "dataflow_jar_main_class" = var.main_class
    "message_deduplication_enabled" = var.message_deduplication_enabled
    "custom_event_time_timestamp_attribute_enabled" = var.custom_event_time_timestamp_attribute_enabled
    "auto_sharding_enabled" = var.auto_sharding_enabled
    "custom_event_time_timestamp_attribute" = var.custom_event_time_timestamp_attribute
    "wait_secs_before_delete" = 300
    // Worker harness starting with
    "number_of_worker_harness_threads" = var.number_of_worker_harness_threads
    # "enable_streaming_engine" = var.enable_streaming_engine # moved to dataflow_classic_template_job to pass it when starting a job
    "dump_heap_on_oom" = var.dump_heap_on_oom
  }
  labels = {
    owner   = var.owner
    number_of_worker_harness_threads = var.number_of_worker_harness_threads
    enable_streaming_engine          = var.enable_streaming_engine
    dump_heap_on_oom                 = var.dump_heap_on_oom
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
    subnetwork = local.subnetwork_name_last_element == "default" ? null : var.subnetwork
  }

//  metadata_startup_script = "echo hi > /tmp/test.txt"

  service_account {
    # Google recommends custom service accounts that have cloud-platform scope and permissions granted via IAM Roles.
    email  = var.service_account
    scopes = ["cloud-platform"]
  }

  provisioner "local-exec" {
    command = <<EOT
      max_retry=40;
      counter=1;
      while [ "$(gcloud compute instances describe --project ${var.project} --zone ${var.zone} ${local.instance} --format='value(metadata.startup-state)')" != "Completed" ] ;
      do
        sleep 5;
        if [ $counter -eq $max_retry ];
        then
          echo "Failed" && break;
        fi;
        if gcloud compute instances get-serial-port-output ${local.instance} --zone ${var.zone} --project ${var.project} | grep startup | grep script | grep Caused |grep Error ;
        then
          echo "java failed" && break;
        fi;
        echo "Waiting for ${local.instance} VM to complete: $counter attempt of $max_retry" ; counter=$(expr $counter + 1);
      done
      gcloud compute instances get-serial-port-output ${local.instance} --zone ${var.zone} --project ${var.project} | grep startup | grep script | grep -v 'INFO  org.apache.beam.' | grep -v 'WARN  org.apache.beam.' | grep -v 'Speed' | grep -v '\-\-:\-\-:\-\-'
      gcloud compute instances describe --project ${var.project} --zone ${var.zone} ${local.instance} --format='value(metadata.startup-state)'
    EOT
  }

  depends_on = [ null_resource.gsutil_upload_dataflow_jar, google_storage_bucket_object.dataflow_jar ]
}
