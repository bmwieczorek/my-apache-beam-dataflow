resource "google_bigquery_table" "view" {
  depends_on = [null_resource.udf]
  project = var.project
  dataset_id = google_bigquery_table.my_table.dataset_id
  table_id = var.view
  deletion_protection = false
  labels = {
    owner   = var.owner
  }
  view {
    query = templatefile("bigquery/view.tpl", {
      dataset = var.dataset,
      project = var.project,
      table = var.table
    })
    use_legacy_sql = false
  }
}
