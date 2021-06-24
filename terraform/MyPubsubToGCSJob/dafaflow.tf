resource "google_storage_bucket_object" "my_bucket_object" {
  name   = "templates/app-image-spec.json"
  source = "../../target/classes/flex-templates/app-image-spec.json"
  bucket = google_storage_bucket.my_bucket.id
}

resource "google_dataflow_flex_template_job" "my_dataflow_flex_job" {
  project               = var.project
  provider              = google-beta
  name                  = var.job
  container_spec_gcs_path = "gs://${google_storage_bucket_object.my_bucket_object.bucket}/${google_storage_bucket_object.my_bucket_object.name}" // id bartek-mypubsubtogcsjob-templates/app-image-spec.json // output_name: templates/app-image-spec.json
  on_delete             = "cancel"
  region                = var.region
  parameters = {
    subscription = google_pubsub_subscription.my_subscription.id
    output = var.output
    workerMachineType = "n2-standard-2"
    workerDiskType = "compute.googleapis.com/projects/${var.project}/zones/${var.zone}/diskTypes/pd-standard"
    diskSizeGb = 200
    serviceAccount = var.service_account
    subnetwork = var.subnetwork
    usePublicIps = false
    experiments = "enable_stackdriver_agent_metrics"
    labels = "{\"owner\":\"bartek\",\"dataflow-template\":\"flex\"}"
  }

  provisioner "local-exec" {
    command = <<EOT
    max_retry=20; for i in $(seq 1 $max_retry); do if [ -z "$(gcloud dataflow jobs list --filter "NAME:${self.name} AND STATE=Running" --format 'value(JOB_ID)' --region ${self.region})" ]; then [[ i -eq $max_retry ]] && echo "Failed to reach running state within $max_retry retries" && break; echo "waiting for job to be in running state"; sleep 10; else echo "Running"; break; fi; done
    EOT
  }

  provisioner "local-exec" {
    when    = destroy
    command = <<EOT
      max_retry=50; counter=1; until ! [ -z "$(gcloud dataflow jobs list --filter "NAME:${self.name} AND (STATE=Cancelling OR STATE=Running)" --format 'value(JOB_ID)' --region "${self.region}")" ] ; do sleep 10; [[ counter -eq $max_retry ]] && echo "Failed" && break; echo "Wating for job to be cancelled: $counter attempt" ; ((counter++)); done
    EOT
  }
}