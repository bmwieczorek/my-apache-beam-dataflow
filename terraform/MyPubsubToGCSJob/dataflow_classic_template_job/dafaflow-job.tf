locals {
  dataflow_start_time = formatdate("YYYY-MM-DD'T'hh:mm:ss'.000Z'", timestamp())
  static_labels = {
    owner                            = var.owner
    dataflow_template                = "classic"
    number_of_worker_harness_threads = var.number_of_worker_harness_threads
    enable_streaming_engine          = var.enable_streaming_engine
    dump_heap_on_oom                 = var.dump_heap_on_oom
  }
  experiments_labels = { for s in var.experiments: "additional_experiments${index(var.experiments, s)}" => replace(s, "=", "--")}
  labels = merge(local.static_labels, local.experiments_labels)
}

resource "google_dataflow_job" "job" {
  count                 = var.dataflow_classic_template_enabled ? 1 : 0
  project               = var.project
  name                  = var.job_name
  temp_gcs_location     = "gs://${var.bucket}/temp"
  template_gcs_path     = var.template_gcs_path
  service_account_email = var.service_account
  network               = var.network
  subnetwork            = var.subnetwork == "default" ? null : var.subnetwork
  max_workers           = var.max_workers
  //  num_workers           = 1 // num_workers not supported by google_dataflow_job, needs to be defined in vm startup script
  //  on_delete             = "cancel"
  on_delete             = "drain"
  ip_configuration      = "WORKER_IP_PRIVATE"
  region                = var.region
  machine_type          = "n1-standard-2"
  // enable_streaming_engine = true //  enable_streaming_engine not supported by google_dataflow_job streaming engine enables automatically autoscaling
  parameters = {
    output         = "gs://${var.bucket}/output"
    temp           = "gs://${var.bucket}/temp"
    tableSpec      = var.table_spec
    subscription   = var.subscription
  //    dumpHeapOnOOM = var.dump_heap_on_oom                                // Error: googleapi: Error 400: The workflow could not be created. Causes: Found unexpected parameters: ['dumpHeapOnOOM'
  //    saveHeapDumpsToGcsPath = var.save_heap_dumps_to_gcs_path            // Error: googleapi: Error 400: The workflow could not be created. Causes: Found unexpected parameters:
  //    numberOfWorkerHarnessThreads = var.number_of_worker_harness_threads // Error: syncing pod ... skipping: failed to "StartContainer" for "java-streaming" with CrashLoopBackOff
  //    profilingAgentConfiguration="{ \"APICurated\" : true }"             // Error: googleapi: Error 400: The workflow could not be created. Causes: Found unexpected parameters: ['profilingAgentConfiguration'
  }

//  additional_experiments = ["enable_stackdriver_agent_metrics","enable_google_cloud_profiler","enable_google_cloud_heap_sampling"]
  additional_experiments = var.experiments

  labels = local.labels
  skip_wait_on_job_termination = var.skip_wait_on_job_termination
}
