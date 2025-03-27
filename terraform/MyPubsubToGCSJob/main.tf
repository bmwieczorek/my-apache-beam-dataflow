locals {
  job_base_name                   = "${var.owner}-mypubsubtogcsjob"
  bucket                          = "${var.project}-${local.job_base_name}"
  topic                           = "${var.owner}-topic"
  subscription                    = "${local.topic}-sub"
  dataset                         = replace(local.job_base_name, "-", "_")
  table                           = "my_table"
  max_workers                     = 2
#  number_of_worker_harness_threads = 0
  number_of_worker_harness_threads = 8
  enable_streaming_engine         = true
  dump_heap_on_oom                = true
  labels = {
    owner = var.owner
  }
  experiments                     = ["enable_stackdriver_agent_metrics", "enable_google_cloud_profiler", "enable_google_cloud_heap_sampling", "disable_runner_v2", "disableStringSetMetrics"]
  jar_version                     = element(regex("(\\d+(\\.\\d+){0,2}(-SNAPSHOT)?)", basename(tolist(fileset(path.module, "../../target/my-*.jar"))[0])),0)
  ts                              = formatdate("YYYY_MM_DD__hh_mm_ss", timestamp())
  job_name_suffix                 =  ""
#  job_name_suffix                 =  "-${local.ts}"
#  job_name_suffix                 = replace(local.ts, local.ts, "")
//  job_name                        = replace(lower("${local.job_base_name}-${local.jar_version}"), ".", "_")

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
  message_deduplication_enabled     = var.dataflow_message_deduplication_enabled
  custom_event_time_timestamp_attribute_enabled = var.dataflow_custom_event_time_timestamp_attribute_enabled
  job_base_name                     = local.job_base_name
  network                           = var.network
//  network             = data.google_compute_network.network.self_link
  subnetwork                        = var.subnetwork == "default" ? null : var.subnetwork
//  subnetwork          = data.google_compute_subnetwork.subnetwork.self_link
  service_account                   = var.service_account
//  dataflow_jar_local_path           = "../../target/my-apache-beam-dataflow-0.1-SNAPSHOT.jar"
  dataflow_jar_local_path           = tolist(fileset(path.module, "../../target/my-*.jar"))[0]
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
  template_gcs_path                 = var.recalculate_template ? module.dataflow_classic_template.template_gcs_path : "gs://${local.bucket}/templates/${local.job_base_name}-template"
  table_spec                        = "${google_bigquery_table.table.dataset_id}.${google_bigquery_table.table.table_id}"
  job_name                          = "${local.job_base_name}${local.job_name_suffix}"
  subscription                      = google_pubsub_subscription.my_subscription.id
  max_workers                       = local.max_workers
  experiments                       = local.experiments
  skip_wait_on_job_termination      = var.skip_wait_on_job_termination
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
  job                               = local.job_base_name
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
  job_base_name = local.job_base_name
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
  job_base_name      = local.job_base_name
  job_name           = var.dataflow_classic_template_enabled ? module.dataflow_classic_template_job.job_name : module.dataflow_flex_template.job_name
  job_id             = var.dataflow_classic_template_enabled ? module.dataflow_classic_template_job.job_id : module.dataflow_flex_template.job_id

  // workaround to wait for job to be created
  module_depends_on  = [module.dataflow_classic_template_job]
}