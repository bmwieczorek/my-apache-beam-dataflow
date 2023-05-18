locals {
  ts     = formatdate("YYYYMMDDhhmmss", timestamp())
  labels = {
    owner = var.owner
  }
}

resource "google_bigquery_dataset" "dataset" {
  project                    = var.project
  dataset_id                 = var.dataset
  friendly_name              = "My dataset friendly name"
  description                = "My dataset description"
  delete_contents_on_destroy = true // deletes logging sink table when destroying the dataset
  labels                     = local.labels
}

resource "google_bigquery_table" "table" {
  project             = var.project
  dataset_id          = google_bigquery_dataset.dataset.dataset_id
  table_id            = var.table
  labels              = local.labels
  deletion_protection = false
  schema              = file(var.table_schema_file)
  /*
    schema = <<EOF
  [
    {
      "mode": "REQUIRED",
      "name": "id",
      "type": "STRING",
      "description" : "The id"
    },
    {
      "mode": "REQUIRED",
      "name": "creation_timestamp",
      "type": "TIMESTAMP",
      "description" : "The creation timestamp"
    },
    {
      "mode": "REQUIRED",
      "name": "expiration_date",
      "type": "DATE",
      "description" : "The expiration date"
    }
  ]
  EOF
  */

}

resource "google_storage_bucket_object" "load_file" {
  name   = "bigquery/${var.load_file}"
  source = "bigquery/${var.load_file}"
  bucket = var.bucket
}

// bq load --source_format NEWLINE_DELIMITED_JSON bartek_dataset.mysubscription_table gs://${GCP_PROJECT}-bartek-mybqreadwritejob/bigquery/mysubscription_table.json
// bq load --source_format AVRO --use_avro_logical_types bartek_dataset.mysubscription_table gs://${GCP_PROJECT}-bartek-mybqreadwritejob/output/year=53139/month=11/day=27/hour=17/26-currTs-1639594742676-winMaxTs--290308-12-21T20_00_59.999Z-paneTiming-ON_TIME-shard-0-of-1.avro
// bq load --source_format=PARQUET --parquet_enable_list_inference bartek_dataset.mysubscription_table  "gs://${GCP_PROJECT}-bartek-mybqreadwritejob/output/year=2021/month=03/day=03/hour=10/10-currTs-1639640027071-winMaxTs--290308-12-21T20_00_59.999Z-paneTiming-ON_TIME-shard-0-of-1.avro"

// bq mkdef --autodetect --source_format=PARQUET gs://${GCP_PROJECT}-bartek-mybqreadwritejob/output/*.parquet  > mysubscription_table_ext.json
// bq mk --external_table_definition=mysubscription_table_ext.json bartek_dataset.mysubscription_table_ext
// bq query --nouse_legacy_sql 'SELECT * FROM bartek_dataset.mysubscription_table_ext'

// bq mkdef --autodetect --hive_partitioning_mode=AUTO  --hive_partitioning_source_uri_prefix="gs://${GCP_PROJECT}-bartek-mybqreadwritejob/output/" --source_format=PARQUET gs://${GCP_PROJECT}-bartek-mybqreadwritejob/output/*.parquet  > mysubscription_table_ext_hive.json
// bq mk --external_table_definition=mysubscription_table_ext_hive.json bartek_dataset.mysubscription_table_ext_hive.json
// bq query --nouse_legacy_sql 'SELECT * FROM bartek_dataset.mysubscription_table_ext_hive'
// bq query --nouse_legacy_sql 'SELECT * FROM bartek_dataset.mysubscription_table_ext_hive where year=2021'
resource "google_bigquery_job" "bigquery_job" {
  project = var.project
  job_id  = "my_bigquery_job_${local.ts}"
  labels  = local.labels

  load {
    source_uris = [
      "gs://${google_storage_bucket_object.load_file.bucket}/${google_storage_bucket_object.load_file.name}"
    ]
    source_format = "NEWLINE_DELIMITED_JSON"

    destination_table {
      project_id = var.project
      dataset_id = google_bigquery_table.table.dataset_id
      table_id   = google_bigquery_table.table.table_id
    }
    write_disposition = "WRITE_TRUNCATE"
  }

  //  query {
  //    query = "INSERT INTO ${google_bigquery_dataset.my_dataset.dataset_id}.${google_bigquery_table.my_table.table_id} (id,creation_timestamp, expiration_date, numbers) values(\"abc\",TIMESTAMP(\"2021-03-03 03:03:03+00\"),DATE '2021-03-03',[1,2,3]),(\"def\",CURRENT_TIMESTAMP(),CURRENT_DATE(),[5,6,7])"
  //    create_disposition = ""
  //    write_disposition = ""
  //  }
}