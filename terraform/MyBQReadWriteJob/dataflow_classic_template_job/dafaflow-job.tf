locals {
  dataflow_start_time = formatdate("YYYY-MM-DD'T'hh:mm:ss'.000Z'", timestamp())
}

resource "google_dataflow_job" "job" {
  project               = var.project
  name                  = "${var.job}-${var.expiration_date}"
  temp_gcs_location     = "gs://${var.bucket}/temp"
  template_gcs_path     = var.template_gcs_path
  service_account_email = var.service_account
  network               = var.network
  subnetwork            = var.subnetwork == "default" ? null : var.subnetwork
  max_workers           = 3
  on_delete             = "cancel"
  ip_configuration      = "WORKER_IP_PRIVATE"
  region                = var.region
  machine_type          = "n1-standard-1"
  parameters = {
    expirationDate = var.expiration_date
    tableSpec = var.table_spec
//    flexRSGoal = "COST_OPTIMIZED" // flexRSGoal flag no supported when starting a job (need to be added when creating template)
    outputPath         = "gs://${var.bucket}/output"
    tempPath           = "gs://${var.bucket}/temp"
    diskSizeGb         = 200
    workerDiskType     = "compute.googleapis.com/projects/sab-eda-01-8302/zones/us-central1-b/diskTypes/pd-ssd"
  }
  additional_experiments = ["disable_runner_v2","shuffle_mode=appliance","enable_stackdriver_agent_metrics"]
  labels = {
    owner   = var.owner
    dataflow_template = "classic"
  }

  provisioner "local-exec" {
    command = <<EOT
      max_retry=40; for i in $(seq 1 $max_retry); do if [ -z "$(gcloud dataflow jobs list --region ${self.region} | grep "${self.name}" | grep Running)" ]; then if [ $i -eq $max_retry ]; then echo "Job ${self.name} failed to reach running state within $max_retry retries" && break; fi; echo "Waiting for job ${self.name} to be in running state"; sleep 5; else echo "Job ${self.name} is running"; break; fi; done
    EOT
#    max_retry=40; for i in $(seq 1 $max_retry); do if [ -z "$(gcloud dataflow jobs list --filter "NAME:${self.name} AND STATE=Running" --format 'value(JOB_ID)' --region ${self.region})" ]; then if [ $i -eq $max_retry ]; then echo "Failed to reach running state within $max_retry retries" && break; fi; echo "Waiting for job to be in running state"; sleep 5; else echo "Running"; break; fi; done
  }

  provisioner "local-exec" {
    when    = destroy
    command = <<EOT
      max_retry=40; counter=1; until ! [ -z "$(gcloud dataflow jobs list --region ${self.region} | grep "${self.name}" | grep -e "Cancelling\|Running")" ] ; do sleep 5; if [ $counter -eq $max_retry ]; then echo "Exceeded number of attempts to wait for job ${self.name} to be cancelled" && break; fi; echo "Wating for job ${self.name} to be cancelled: $counter attempt"; counter=$(expr $counter + 1); done
    EOT
#    max_retry=40; counter=1; until ! [ -z "$(gcloud dataflow jobs list --filter "NAME:${self.name} AND (STATE=Cancelling OR STATE=Running)" --format 'value(JOB_ID)' --region "${self.region}")" ] ; do sleep 5; if [ $counter -eq $max_retry ]; then echo "Failed" && break; fi; echo "Wating for job to be cancelled: $counter attempt"; counter=$(expr $counter + 1); done
  }
}
