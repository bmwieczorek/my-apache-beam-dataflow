locals {
  dataset_id                       = "${var.owner}_dataset"
  bucket                           = "${var.project}-${var.owner}"
  external_biglake_table           = "external_biglake_table"
  empty_parquet_file               = "emptyMyNestedRecord.parquet"
  non_empty_parquet_file           = "myNestedRecord.parquet"
  gcs_hive_zero_partition_path     = "year=0000/month=00/day=00/hour=00"
  gcs_hive_non_zero_partition_path = "year=2024/month=09/day=09/hour=09"
  bq_schema_file                   = "myBigQueryNestedParquet.json"
}

resource "google_storage_bucket_object" "empty_parquet_file" {
  name   = "${local.external_biglake_table}/${local.gcs_hive_zero_partition_path}/${local.empty_parquet_file}"
  source = local.empty_parquet_file
  bucket = local.bucket
}

resource "null_resource" "parquet_metadata" {
  triggers = {
    always_run = formatdate("YYYY-MM-DD-hh-mm-ss", timestamp())
  }

  provisioner "local-exec" {
    command = "parquet meta ${local.empty_parquet_file} | grep myRequiredDateTime"
  }
  depends_on = [google_storage_bucket_object.empty_parquet_file]
}

resource "google_bigquery_table" "external_biglake_table" {
  project             = var.project
  dataset_id          = local.dataset_id
  table_id            = local.external_biglake_table
  deletion_protection = false
#   schema = file(var.schema_file)
  schema = null

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"

    parquet_options {
      enable_list_inference = true
    }

    hive_partitioning_options {
      mode              = "CUSTOM"
      source_uri_prefix = "gs://${local.bucket}/${local.external_biglake_table}/{year:STRING}/{month:STRING}/{day:STRING}/{hour:STRING}"
    }

    source_uris = ["gs://${local.bucket}/${local.external_biglake_table}/*.parquet"]
  }

  depends_on = [null_resource.parquet_metadata]
}

resource "null_resource" "bq_show_schema" {
  triggers = {
    always_run = formatdate("YYYY-MM-DD-hh-mm-ss", timestamp())
  }

  provisioner "local-exec" {
    command = "bq show --format=prettyjson ${google_bigquery_table.external_biglake_table.project}:${google_bigquery_table.external_biglake_table.dataset_id}.${google_bigquery_table.external_biglake_table.table_id} | jq '.schema.fields' | jq '.[] | select(.name==\"myRequiredDateTime\")'"
  }
  depends_on = [google_bigquery_table.external_biglake_table]
}

resource "null_resource" "bq_select_external_biglake_table_with_empty_file_only" {
  triggers = {
    always_run = formatdate("YYYY-MM-DD-hh-mm-ss", timestamp())
  }

  provisioner "local-exec" {
    command = "bq query --use_legacy_sql=false 'SELECT * FROM ${google_bigquery_table.external_biglake_table.dataset_id}.${google_bigquery_table.external_biglake_table.table_id} WHERE year is not null'"
  }
  depends_on = [null_resource.bq_show_schema]
}

resource "google_storage_bucket_object" "non_empty_parquet_file" {
  name       = "${local.external_biglake_table}/${local.gcs_hive_non_zero_partition_path}/${local.non_empty_parquet_file}"
  source     = local.non_empty_parquet_file
  bucket     = local.bucket
  depends_on = [null_resource.bq_select_external_biglake_table_with_empty_file_only]
}

resource "null_resource" "bq_select_external_biglake_table_with_non_empty_file" {
  triggers = {
    always_run = formatdate("YYYY-MM-DD-hh-mm-ss", timestamp())
  }

  provisioner "local-exec" {
    command = "bq query --use_legacy_sql=false 'SELECT * FROM ${google_bigquery_table.external_biglake_table.dataset_id}.${google_bigquery_table.external_biglake_table.table_id} WHERE year is not null'"
  }

  depends_on = [null_resource.bq_select_external_biglake_table_with_empty_file_only, google_storage_bucket_object.non_empty_parquet_file]
}


resource "google_storage_bucket_iam_member" "storage_sa_binding" {
  bucket = local.bucket
  role = "roles/storage.objectViewer"
  member = "serviceAccount:bq-internal-sa@gcp-sa-bigquery-condel.iam.gserviceaccount.com"
}

resource "google_storage_bucket_object" "bq_schema_file" {
  name       = local.bq_schema_file
  source     = local.bq_schema_file
  bucket     = local.bucket
}

# curl -X POST \
#  -H "Authorization: Bearer $(gcloud auth print-access-token)" \
#  -H "Content-Type: application/json" \
#  -d '{
#    "tableReference": {
#      "projectId": "my-bq-project",
#      "datasetId": "bartek_dataset",
#      "tableId": "my_rest_table_external_autodetect"
#    },
#    "externalDataConfiguration": {
#	   "sourceUris": ["gs://my-bucket/external_biglake_table/*.parquet"],
#	   "sourceFormat": "PARQUET",
#	   "autodetect": true,
#	   "hivePartitioningOptions": {
#	     "mode": "CUSTOM",
#  	     "sourceUriPrefix": "gs://my-bucket/external_biglake_table/{year:STRING}/{month:STRING}/{day:STRING}/{hour:STRING}",
#        "requirePartitionFilter": true,
#      },
#	   "connectionId": "my-bq-project.us.lakehouse_connection"
#    }
#  }' \
# "https://bigquery.googleapis.com/bigquery/v2/projects/my-bq-project/datasets/bartek_dataset/tables"


# schema="`cat myBigQueryNestedParquet.json`"
#curl -X POST \
#  -H "Authorization: Bearer $(gcloud auth print-access-token)" \
#  -H "Content-Type: application/json" \
#  -d "{
#    \"tableReference\": {
#      \"projectId\": \"my-bq-project\",
#      \"datasetId\": \"bartek_dataset\",
#      \"tableId\": \"my_rest_table_external_no_autodetect_use_schema_file\"
#    },
#    \"schema\": {
#	\"fields\": $schema
#    },
#    \"externalDataConfiguration\": {
#	\"sourceUris\": [\"gs://my-bucket/external_biglake_table/*.parquet\"],
#	\"sourceFormat\": \"PARQUET\",
#	\"autodetect\": false,
#	\"hivePartitioningOptions\": {
#	  \"mode\": \"CUSTOM\",
#  	  \"sourceUriPrefix\": \"gs://my-bucket/external_biglake_table/{year:STRING}/{month:STRING}/{day:STRING}/{hour:STRING}\",
#         \"requirePartitionFilter\": true,
#       },
#	\"connectionId\": \"my-bq-project.us.lakehouse_connection\",
#    }
#  }" \
# https://bigquery.googleapis.com/bigquery/v2/projects/my-bq-project/datasets/bartek_dataset/tables
