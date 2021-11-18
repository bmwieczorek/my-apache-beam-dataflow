locals {
  job                 = "${var.owner}-mybqreadwritejob"
  bucket              = "${var.project}-${var.owner}-${local.job}"
}

data "google_compute_network" "network" {
  project = var.project
  name    = var.network
}

data "google_compute_subnetwork" "subnetwork" {
  project = var.project
  region  = var.region
  name    = var.subnetwork
}

module "storage" {
  source              = "./storage"
  project             = var.project
  owner               = var.owner
  bucket              = local.bucket
}

module "bigquery" {
  source              = "./bigquery"
  project             = var.project
  owner               = var.owner
  bucket              = module.storage.bucket_name
  dataset             = "${var.owner}_dataset"
  table_schema_file   = "../../target/MyBQReadWriteJob.json"
}

module "dataflow_classic_template" {
  source              = "./dataflow_classic_template"
  project             = var.project
  region              = var.region
  zone                = var.zone
  owner               = var.owner
  bucket              = module.storage.bucket_name
  main_class          = "com.bawi.beam.dataflow.MyBQReadWriteJob"
  job                 = local.job
//  network             = var.network
  network             = data.google_compute_network.network.self_link
//  subnetwork          = var.subnetwork
//  subnetwork          = var.subnetwork == "default" ? null : var.subnetwork
  subnetwork          = data.google_compute_subnetwork.subnetwork.self_link
  service_account     = var.service_account
  dataflow_jar_local_path = "../../target/my-apache-beam-dataflow-0.1-SNAPSHOT.jar"
  image               = var.image
}

module "dataflow_classic_template_job" {
  source              = "./dataflow_classic_template_job"
  project             = var.project
  region              = var.region
  zone                = var.zone
  owner               = var.owner
  bucket              = module.storage.bucket_name
  //  network             = var.network
  network             = data.google_compute_network.network.self_link
  //  subnetwork          = var.subnetwork
  //  subnetwork          = var.subnetwork == "default" ? null : var.subnetwork
  subnetwork          = data.google_compute_subnetwork.subnetwork.self_link
  service_account     = var.service_account
  template_gcs_path   = module.dataflow_classic_template.template_gcs_path
  job                 = local.job
  expiration_date     = "2021-03-03"
}

module "dashboards" {
  source              = "./dashboards"
  project             = var.project
  dataflow_job_id     = module.dataflow_classic_template_job.job_id
  job                 = local.job
  logs_based_metric_type = module.alerting.logs_based_metric_type
}

module "alerting" {
  source               = "./alerting"
  project              = var.project
  owner                = var.owner
  notification_email   = var.notification_email
  job                  = local.job
  logs_based_metrics_message_pattern = "Created MySubscription"

  // workaround to wait for job to be created
  module_depends_on    = [module.dataflow_classic_template_job]
}

module "logging-sink" {
  source = "./logging-sink"
  project             = var.project
  owner               = var.owner
  bucket              = module.storage.bucket_name
  job                 = local.job
  dataflow_start_time = module.dataflow_classic_template_job.dataflow_start_time
  log_message_pattern = "Worker pool stopped."
}
