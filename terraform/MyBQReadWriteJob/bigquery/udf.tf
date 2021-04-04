# when trigger detects a change in template it will re-run the provisioner

locals {
  udf_create = templatefile("udf_create.tpl", {
    project = var.project,
    dataset = google_bigquery_dataset.my_dataset.dataset_id
  })
  udf_destroy = templatefile("udf_destroy.tpl", {
    project = var.project,
    dataset = google_bigquery_dataset.my_dataset.dataset_id
  })
}

resource "null_resource" "udf" {

  depends_on = [google_bigquery_table.my_table]

  triggers = {
    udf = local.udf_create
  }

  provisioner "local-exec" {
    interpreter = [
      "bq",
      "query",
      "--use_legacy_sql=false",
      "--project_id=${var.project}"
    ]
    command = local.udf_create
  }

  provisioner "local-exec" {
    when = destroy
    interpreter = [
      "bq",
      "query",
      "--use_legacy_sql=false",
      "--project_id=${var.project}"
    ]
    command = local.udf_destroy
  }
}
