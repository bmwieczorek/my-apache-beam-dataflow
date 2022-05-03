resource "google_bigquery_routine" "my_sum_udf" {
  project         = var.project
  dataset_id      = google_bigquery_dataset.dataset.dataset_id
  routine_id      = "my_sum"
  routine_type    = "SCALAR_FUNCTION"
//  language        = "SQL"
  definition_body = "((SELECT SUM(x) FROM UNNEST(arr) AS x))"
//  definition_body = "((SELECT SUM(x) FROM UNNEST(arr) AS x WHERE MOD(x, 2) = 0))"
  arguments {
    name = "arr"
    argument_kind = "ANY_TYPE"
  }
  return_type = "{\"typeKind\" :  \"INT64\"}"
}

resource "google_bigquery_routine" "my_even_numbers_udf" {
  project         = var.project
  dataset_id      = google_bigquery_dataset.dataset.dataset_id
  routine_id      = "my_even_numbers"
  routine_type    = "SCALAR_FUNCTION"
  language        = "SQL"
  definition_body = "((SELECT ARRAY_AGG(x) FROM UNNEST(arr) AS x WHERE MOD(x, 2) = 0))"
  arguments {
    name = "arr"
    argument_kind = "ANY_TYPE"
  }
}

//CREATE TEMP FUNCTION my_elements(arr ANY TYPE) AS ((SELECT ARRAY_AGG(x) FROM UNNEST(arr) AS x WHERE MOD(x, 2) = 0));
//--SELECT my_elements(numbers) as my_elements, numbers FROM `sab-eda-01-8302.bartek_dataset.mysubscription_view`;
//SELECT my_elements[SAFE_OFFSET(0)] as n0, my_elements[SAFE_OFFSET(1)] as n1, numbers FROM (SELECT my_elements(numbers) as my_elements, numbers FROM `sab-eda-01-8302.bartek_dataset.mysubscription_view`);

# when trigger detects a change in template it will re-run the provisioner
//locals {
//  udf_create = <<-EOF
//      CREATE OR REPLACE FUNCTION `${var.project}.${google_bigquery_dataset.my_dataset.dataset_id}.my_sum` (arr ANY TYPE) AS (
//        (SELECT SUM(x) FROM UNNEST(arr) AS x)
//      );
//  EOF
//
//  udf_destroy = <<-EOF
//      DROP FUNCTION IF EXISTS `${var.project}.${google_bigquery_dataset.my_dataset.dataset_id}.my_sum`;
//  EOF
//}
//
//resource "null_resource" "my_sum_udf" {
//  depends_on = [google_bigquery_table.my_table]
//  triggers = {
//    udf = local.udf_create
//  }
//
//  provisioner "local-exec" {
//    interpreter = [
//      "bq",
//      "query",
//      "--use_legacy_sql=false"
//    ]
//    command = local.udf_create
//  }
//
//  provisioner "local-exec" {
//    when = destroy
//    interpreter = [
//      "bq",
//      "query",
//      "--use_legacy_sql=false"
//    ]
//    command = local.udf_destroy
//  }
//}
