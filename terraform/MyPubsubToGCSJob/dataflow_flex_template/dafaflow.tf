locals {
  dataflow_start_time = formatdate("YYYY-MM-DD'T'hh:mm:ss'.000Z'", timestamp())
  static_labels = {
    owner   = var.owner
    dataflow_template = "flex"
    number_of_worker_harness_threads = var.number_of_worker_harness_threads
    enable_streaming_engine          = var.enable_streaming_engine
    dump_heap_on_oom                 = var.dump_heap_on_oom
  }
  additional_experiments_labels = { for s in var.experiments: "additional_experiments${index(var.experiments, s)}" => replace(s, "=", "--")}
  merged_labels = merge(local.static_labels, local.additional_experiments_labels)
  labels = join(",", [for key, value in local.merged_labels : "\"${key}\":\"${value}\""])
}

resource "google_storage_bucket_object" "app_image_spec_json" {
  count  = var.dataflow_flex_template_enabled ? 1 : 0
  name   = "templates/app-image-spec.json"
  source = "../../target/classes/flex-templates/app-image-spec.json"
  bucket = var.bucket
}

resource "google_dataflow_flex_template_job" "job" {
  count                 = var.dataflow_flex_template_enabled ? 1 : 0
  project               = var.project
  provider              = google-beta
  name                  = var.job
  container_spec_gcs_path = "gs://${google_storage_bucket_object.app_image_spec_json[0].bucket}/${google_storage_bucket_object.app_image_spec_json[0].name}" // id bartek-mypubsubtogcsjob-templates/app-image-spec.json // output_name: templates/app-image-spec.json
  on_delete             = "cancel"
  region                = var.region
  parameters = {
    subscription      = var.subscription
    output            = "gs://${var.bucket}/output"
    temp              = "gs://${var.bucket}/temp"
    workerMachineType = "n2-standard-2"
    workerDiskType    = "compute.googleapis.com/projects/${var.project}/zones/${var.zone}/diskTypes/pd-standard"
    diskSizeGb        = 200
    serviceAccount    = var.service_account
    network           = var.network
    subnetwork        = var.subnetwork == "default" ? null : var.subnetwork
    usePublicIps      = false
    maxNumWorkers     = var.max_workers
//    experiments       = "enable_stackdriver_agent_metrics,enable_google_cloud_profiler,enable_google_cloud_heap_sampling,num_pubsub_keys=2000"
    experiments       = join(",", var.experiments)
//    labels            = "{\"owner\":\"bartek\",\"dataflow-template\":\"flex\"}"
    labels            = "{${local.labels}}"
    workerLogLevelOverrides = "{ \"org.apache.beam\": \"DEBUG\" }"
    dumpHeapOnOOM     = true
//    profilingAgentConfiguration = "{ \"APICurated\" : true }"
    numberOfWorkerHarnessThreads = var.number_of_worker_harness_threads
    enableStreamingEngine = var.enable_streaming_engine
    dumpHeapOnOOM = var.dump_heap_on_oom
  }

  provisioner "local-exec" {
    command = <<EOT
    max_retry=20; for i in $(seq 1 $max_retry); do if [ -z "$(gcloud dataflow jobs list --filter "NAME:${self.name} AND STATE=Running" --format 'value(JOB_ID)' --region ${self.region})" ]; then if [ $i -eq $max_retry ]; then echo "Failed to reach running state within $max_retry retries" && break; fi; echo "Waiting for job to be in running state"; sleep 10; else echo "Running"; break; fi; done
    EOT
  }

  provisioner "local-exec" {
    when    = destroy
    command = <<EOT
      max_retry=50; counter=1; until ! [ -z "$(gcloud dataflow jobs list --filter "NAME:${self.name} AND (STATE=Cancelling OR STATE=Running)" --format 'value(JOB_ID)' --region "${self.region}")" ] ; do sleep 10; if [ $counter -eq $max_retry ]; then echo "Failed" && break; fi; echo "Wating for job to be cancelled: $counter attempt"; counter=$(expr $counter + 1); done
    EOT
  }
}