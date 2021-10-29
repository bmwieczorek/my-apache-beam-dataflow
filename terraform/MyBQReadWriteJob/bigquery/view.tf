resource "google_bigquery_table" "view" {
  depends_on = [google_bigquery_routine.my_sum_udf]
  project = var.project
  dataset_id = google_bigquery_table.table.dataset_id
  table_id = var.view
  deletion_protection = false
  labels = {
    owner   = var.owner
  }
  view {
    query = templatefile("${path.module}/view.tpl", {
      dataset = var.dataset,
      project = var.project,
      table = var.table
    })
    use_legacy_sql = false
  }
}
