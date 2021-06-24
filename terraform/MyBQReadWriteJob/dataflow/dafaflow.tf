resource "google_storage_bucket_object" "my_bucket_object" {
  name   = "templates/${var.job}-template_metadata"
  source = "../../../dataflow-templates/${var.job}-template_metadata"
  bucket = var.bucket
}

resource "google_dataflow_job" "my_dataflow_job" {
  depends_on = [google_compute_instance.compute_template]
  project               = var.project
  name                  = "${var.job}-${var.expiration_date}"
  temp_gcs_location     = "gs://${var.bucket}/temp"
  template_gcs_path     = "gs://${var.bucket}/templates/${var.job}-template"
  service_account_email = var.service_account
  subnetwork            = var.subnetwork
  max_workers           = 3
  on_delete             = "cancel"
  ip_configuration      = "WORKER_IP_PRIVATE"
  region                = var.region
  machine_type          = "n1-standard-1"
  parameters = {
    expirationDate = var.expiration_date
  }
  additional_experiments = ["enable_stackdriver_agent_metrics"]
  labels = {
    owner   = var.owner
    dataflow_template = "classic"
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
