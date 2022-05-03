resource "google_bigquery_table" "view_with_sum" {
  depends_on = [google_bigquery_routine.my_sum_udf]
  project = var.project
  dataset_id = google_bigquery_table.table.dataset_id
  table_id = var.view
  deletion_protection = false
  labels = {
    owner   = var.owner
  }
  view {
    query = templatefile("${path.module}/view_with_sum.tpl", {
      dataset = var.dataset,
      project = var.project,
      table = var.table
    })
    use_legacy_sql = false
  }

  // update to add fields description, cannot change types
  provisioner "local-exec" {
    command = "bq update ${var.project}:${var.dataset}.${var.view} ${path.module}/view_with_sum_schema_with_description.json"
  }
}

resource "google_bigquery_table" "view_with_numbers" {
  depends_on = [google_bigquery_routine.my_even_numbers_udf]
  project = var.project
  dataset_id = google_bigquery_table.table.dataset_id
  table_id = "my_view_with_numbers"
  deletion_protection = false
  labels = {
    owner   = var.owner
  }
  view {
    query = templatefile("${path.module}/view_with_even_numbers.tpl", {
      dataset = var.dataset,
      project = var.project,
      table = var.table
    })
    use_legacy_sql = false
  }
}
