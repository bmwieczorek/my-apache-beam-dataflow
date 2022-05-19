locals {
  job_name_base                   = "${var.owner}-mypubsubtogcsjob"
  bucket                          = "${var.project}-${local.job_name_base}"
  topic                           = "${var.owner}-topic"
  subscription                    = "${local.topic}-sub"
  dataset                         = "bartek_dataset"
  table                           = "my_table"
  max_workers                     = 3
  number_of_worker_harness_threads = 8
  enable_streaming_engine         = true
  dump_heap_on_oom                = true
  labels = {
    owner = var.owner
  }
  experiments                     = ["enable_stackdriver_agent_metrics","enable_google_cloud_profiler","enable_google_cloud_heap_sampling"]
}

//data "google_compute_network" "network" {
//  project = var.project
//  name    = var.network
//}
//
//data "google_compute_subnetwork" "subnetwork" {
//  project = var.project
//  region  = var.region
//  name    = var.subnetwork
//}

module "dataflow_classic_template" {
  source                            = "./dataflow_classic_template"
  dataflow_classic_template_enabled = var.dataflow_classic_template_enabled
  project                           = var.project
  region                            = var.region
  zone                              = var.zone
  owner                             = var.owner
//  bucket              = module.storage.bucket_name
  bucket                            = google_storage_bucket.my_bucket.name
  main_class                        = "com.bawi.beam.dataflow.MyPubsubToGCSJob"
  job                               = local.job
  network                           = var.network
//  network             = data.google_compute_network.network.self_link
  subnetwork                        = var.subnetwork == "default" ? null : var.subnetwork
//  subnetwork          = data.google_compute_subnetwork.subnetwork.self_link
  service_account                   = var.service_account
  dataflow_jar_local_path           = "../../target/my-apache-beam-dataflow-0.1-SNAPSHOT.jar"
  image                             = var.image
  number_of_worker_harness_threads  = local.number_of_worker_harness_threads
  enable_streaming_engine           = local.enable_streaming_engine
  dump_heap_on_oom                  = local.dump_heap_on_oom
}

module "dataflow_classic_template_job" {
  source                            = "./dataflow_classic_template_job"
  dataflow_classic_template_enabled = var.dataflow_classic_template_enabled
  project                           = var.project
  region                            = var.region
  zone                              = var.zone
  owner                             = var.owner
  bucket                            = google_storage_bucket.my_bucket.name
  network                           = var.network
  subnetwork                        = var.subnetwork == "default" ? null : var.subnetwork
  service_account                   = var.service_account
  template_gcs_path                 = module.dataflow_classic_template.template_gcs_path
//  template_gcs_path                 = "gs://${local.bucket}/templates/${local.job}-template"
  job_name                          = local.job
//  job_name                          = "${local.job}-v3"
  subscription                      = google_pubsub_subscription.my_subscription.id
  max_workers                       = local.max_workers
  experiments                       = local.experiments
  number_of_worker_harness_threads  = local.number_of_worker_harness_threads
  enable_streaming_engine           = local.enable_streaming_engine
  dump_heap_on_oom                  = local.dump_heap_on_oom
}

module "dataflow_flex_template" {
  source                            = "./dataflow_flex_template"
  dataflow_flex_template_enabled    = !var.dataflow_classic_template_enabled
  project                           = var.project
  region                            = var.region
  zone                              = var.zone
  owner                             = var.owner
  bucket                            = google_storage_bucket.my_bucket.name
  network                           = var.network
  subnetwork                        = var.subnetwork == "default" ? null : var.subnetwork
  service_account                   = var.service_account
  job                               = local.job_name_base
  subscription                      = google_pubsub_subscription.my_subscription.id
  max_workers                       = local.max_workers
  experiments                       = local.experiments
  number_of_worker_harness_threads  = local.number_of_worker_harness_threads
  enable_streaming_engine           = local.enable_streaming_engine
  dump_heap_on_oom                  = local.dump_heap_on_oom
}

module "dashboards" {
  source        = "./dashboards"
  project       = var.project
  job_name_base = local.job_name_base
  job_name      = var.dataflow_classic_template_enabled ? module.dataflow_classic_template_job.job_name : module.dataflow_flex_template.job_name
  job_id        = var.dataflow_classic_template_enabled ? module.dataflow_classic_template_job.job_id : module.dataflow_flex_template.job_id
  topic         = local.topic
  subscription  = local.subscription
}

module "alerting" {
  source             = "./alerting"
  project            = var.project
  owner              = var.owner
  notification_email = var.notification_email
  job_name_base      = local.job_name_base
  job_name           = var.dataflow_classic_template_enabled ? module.dataflow_classic_template_job.job_name : module.dataflow_flex_template.job_name
  job_id             = var.dataflow_classic_template_enabled ? module.dataflow_classic_template_job.job_id : module.dataflow_flex_template.job_id

  // workaround to wait for job to be created
  module_depends_on  = [module.dataflow_classic_template_job]
}