output "template_gcs_path" {
  value = local.template_gcs_path
  depends_on = [google_compute_instance.dataflow_classic_template_compute]
}