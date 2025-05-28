locals {
  biglake_table                    = "biglake_table"
  external_parquet_table           = "external_parquet_table"
  external_avro_table              = "external_avro_table"
  gcs_parquet_files_root_path      = "parquet"
  gcs_avro_files_root_path         = "avro"
  empty_parquet_file               = "myEmptyNestedRecord.parquet"
  non_empty_parquet_file           = "myNestedRecord.parquet"
  empty_avro_file                  = "myEmptyNestedRecord.snappy.avro"
  non_empty_avro_file              = "myNestedRecord.snappy.avro"
  gcs_hive_zero_partition_path     = "year=0000/month=00/day=00/hour=00"
  gcs_hive_non_zero_partition_path = "year=2024/month=09/day=09/hour=09"
  bq_schema_file                   = "myNestedParquetBigQuerySchema.json"
}

resource "google_storage_bucket" "bucket" {
  project = var.project
  name   = "${var.project}-${var.owner}-biglake-external-table"
  location = var.location
}

resource "google_bigquery_dataset" "dataset" {
  project    = var.project
  dataset_id = "${var.owner}_biglake_external"
  location   = var.location
}

###########
# PARQUET #
###########

resource "null_resource" "show_local_parquet_metadata" {
  triggers = {
    always_run = formatdate("YYYY-MM-DD-hh-mm-ss", timestamp())
  }

  provisioner "local-exec" {
    command = "parquet meta ${local.empty_parquet_file} | grep -i time"
  }
}

data "external" "bq_connection_id_sa" {
  program = ["bash", "get_connection_id_sa.sh", var.bq_project, var.location]
}

data "external" "bq_connection_id" {
  program = ["bash", "get_connection_id.sh", var.bq_project, var.location]
}

resource "google_storage_bucket_object" "empty_parquet_file" {
  name   = "${local.gcs_parquet_files_root_path}/${local.gcs_hive_zero_partition_path}/${local.empty_parquet_file}"
  source = local.empty_parquet_file
  bucket = google_storage_bucket.bucket.name
}

resource "google_storage_bucket_iam_member" "storage_to_bq_conn_id_sa_binding" {
  bucket = google_storage_bucket.bucket.name
  role = "roles/storage.objectViewer"
  member = "serviceAccount:${data.external.bq_connection_id_sa.result.serviceAccountId}"
}

resource "google_bigquery_table" "biglake_with_autodetect_schema_automatic_metadata_refresh" {
  project             = var.project
  dataset_id          = google_bigquery_dataset.dataset.dataset_id
  table_id            = "${local.biglake_table}_with_autodetect_schema_automatic_metadata_refresh"
  deletion_protection = false
  # schema = file(local.bq_schema_file)
  schema = null
  max_staleness = "0-0 7 0:0:0" # 7 days

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    # metadata_cache_mode = "MANUAL" # CALL BQ.REFRESH_EXTERNAL_METADATA_CACHE('myproject.test_db.test_table')
    metadata_cache_mode = "AUTOMATIC" # requires max_staleness #  googleapi: Error 400: maxStaleness must be specified when MetadataCacheMode is AUTOMATIC, invalid
    connection_id = "${var.bq_project}.us.${data.external.bq_connection_id.result.connectionId}"

    parquet_options {
      enable_list_inference = true
    }

    hive_partitioning_options {
      mode              = "CUSTOM"
      source_uri_prefix = "gs://${google_storage_bucket.bucket.name}/${local.gcs_parquet_files_root_path}/{year:STRING}/{month:STRING}/{day:STRING}/{hour:STRING}"
    }

    source_uris = ["gs://${google_storage_bucket.bucket.name}/${local.gcs_parquet_files_root_path}/*.parquet"]
  }

  depends_on = [
    google_storage_bucket_iam_member.storage_to_bq_conn_id_sa_binding, # Error: googleapi: Error 403: Access Denied: BigQuery BigQuery: Permission denied while globbing file pattern. bqcx-xxxx-yyy@gcp-sa-bigquery-condel.iam.gserviceaccount.com does not have storage.objects.list access to the Google Cloud Storage bucket. Permission 'storage.objects.list'. Please make sure gs://bucket/external_biglake_table/*.parquet is accessible via appropriate IAM roles, e.g. Storage Object Viewer or Storage Object Creator.
    google_storage_bucket_object.empty_parquet_file #Error: googleapi: Error 400: Error while reading table: biglake_table_autodetect_schema_automatic_metadata_refresh, error message: Failed to expand table biglake_table_autodetect_schema_automatic_metadata_refresh with file pattern gs://bucket/parquet/*.parquet: matched no files. File: gs://bucket/parquet/*.parquet, invalid
  ]
}

resource "null_resource" "bq_show_schema_biglake_with_autodetect_schema_automatic_metadata_refresh" {
  triggers = {
    always_run = formatdate("YYYY-MM-DD-hh-mm-ss", timestamp())
  }

  provisioner "local-exec" {
    command = "bq show --format=prettyjson ${google_bigquery_dataset.dataset.project}:${google_bigquery_dataset.dataset.dataset_id}.${google_bigquery_table.biglake_with_autodetect_schema_automatic_metadata_refresh.table_id} | jq '.schema.fields' | jq '.[] | select(.name==\"myRequiredDateTime\")'"
  }
  depends_on = [
    google_bigquery_table.biglake_with_autodetect_schema_automatic_metadata_refresh
  ]
}

resource "null_resource" "bq_select_biglake_with_autodetect_schema_automatic_metadata_refresh_with_empty_file_only" {
  triggers = {
    always_run = formatdate("YYYY-MM-DD-hh-mm-ss", timestamp())
  }

  provisioner "local-exec" {
    command = "bq query --use_legacy_sql=false 'SELECT count(*) AS cnt FROM ${google_bigquery_dataset.dataset.project}.${google_bigquery_dataset.dataset.dataset_id}.${google_bigquery_table.biglake_with_autodetect_schema_automatic_metadata_refresh.table_id} WHERE year is not null'"
  }
}

resource "google_storage_bucket_object" "non_empty_parquet_file" {
  name       = "${local.gcs_parquet_files_root_path}/${local.gcs_hive_non_zero_partition_path}/${local.non_empty_parquet_file}"
  source     = local.non_empty_parquet_file
  bucket     = google_storage_bucket.bucket.name
  depends_on = [
    null_resource.bq_select_biglake_with_autodetect_schema_automatic_metadata_refresh_with_empty_file_only
  ]
}

resource "null_resource" "bq_select_biglake_with_autodetect_schema_automatic_metadata_refresh_with_non_empty_file" {
  triggers = {
    always_run = formatdate("YYYY-MM-DD-hh-mm-ss", timestamp())
  }

  provisioner "local-exec" {
    command = "bq query --use_legacy_sql=false 'SELECT * FROM ${google_bigquery_dataset.dataset.project}.${google_bigquery_dataset.dataset.dataset_id}.${google_bigquery_table.biglake_with_autodetect_schema_automatic_metadata_refresh.table_id} WHERE year is not null'"
  }

  depends_on = [
    google_storage_bucket_object.non_empty_parquet_file
  ]
}

resource "google_bigquery_table" "biglake_with_explicit_schema_manual_metadata_refresh" {
  project             = var.project
  dataset_id          = google_bigquery_dataset.dataset.dataset_id
  table_id            = "${local.biglake_table}_with_explicit_schema_manual_metadata_refresh"
  deletion_protection = false
  schema = file(local.bq_schema_file)
  # schema = null
  max_staleness = "0-0 7 0:0:0" # 7 days


  external_data_configuration {
    autodetect    = false
    source_format = "PARQUET"
    metadata_cache_mode = "MANUAL" # CALL BQ.REFRESH_EXTERNAL_METADATA_CACHE('myproject.test_db.test_table')
    # metadata_cache_mode = "AUTOMATIC" # requires max_staleness #  googleapi: Error 400: maxStaleness must be specified when MetadataCacheMode is AUTOMATIC, invalid
    connection_id = "${var.bq_project}.us.${data.external.bq_connection_id.result.connectionId}"

    parquet_options {
      enable_list_inference = true
    }

    hive_partitioning_options {
      mode              = "CUSTOM"
      source_uri_prefix = "gs://${google_storage_bucket.bucket.name}/${local.gcs_parquet_files_root_path}/{year:STRING}/{month:STRING}/{day:STRING}/{hour:STRING}"
    }

    source_uris = ["gs://${google_storage_bucket.bucket.name}/${local.gcs_parquet_files_root_path}/*.parquet"]
  }

  depends_on = [
    google_storage_bucket_iam_member.storage_to_bq_conn_id_sa_binding, # Error: googleapi: Error 403: Access Denied: BigQuery BigQuery: Permission denied while globbing file pattern. bqcx-xxxx-yyy@gcp-sa-bigquery-condel.iam.gserviceaccount.com does not have storage.objects.list access to the Google Cloud Storage bucket. Permission 'storage.objects.list'. Please make sure gs://bucket/external_biglake_table/*.parquet is accessible via appropriate IAM roles, e.g. Storage Object Viewer or Storage Object Creator.
    google_storage_bucket_object.empty_parquet_file #Error: googleapi: Error 400: Error while reading table: biglake_with_explicit_schema_manual_metadata_refresh, error message: Failed to expand table biglake_with_explicit_schema_manual_metadata_refresh with file pattern gs://bucket/parquet/*.parquet: matched no files. File: gs://bucket/parquet/*.parquet, invalid
  ]
}

resource "null_resource" "refresh_metadata_cache" {
  triggers = {
    always_run = formatdate("YYYY-MM-DD-hh-mm-ss", timestamp())
  }

  provisioner "local-exec" {
    command = "bq query --use_legacy_sql=false 'CALL BQ.REFRESH_EXTERNAL_METADATA_CACHE(\"${google_bigquery_dataset.dataset.project}.${google_bigquery_dataset.dataset.dataset_id}.${google_bigquery_table.biglake_with_explicit_schema_manual_metadata_refresh.table_id}\")'"
  }

  depends_on = [google_bigquery_table.biglake_with_explicit_schema_manual_metadata_refresh]
}

resource "null_resource" "bq_show_schema_biglake_with_explicit_schema_manual_metadata_refresh" {
  triggers = {
    always_run = formatdate("YYYY-MM-DD-hh-mm-ss", timestamp())
  }

  provisioner "local-exec" {
    command = "bq show --format=prettyjson ${google_bigquery_dataset.dataset.project}:${google_bigquery_dataset.dataset.dataset_id}.${google_bigquery_table.biglake_with_explicit_schema_manual_metadata_refresh.table_id} | jq '.schema.fields' | jq '.[] | select(.name==\"myRequiredDateTime\")'"
  }
  depends_on = [
    google_bigquery_table.biglake_with_explicit_schema_manual_metadata_refresh,
  ]
}

resource "null_resource" "bq_select_biglake_with_explicit_schema_manual_metadata_refresh_with_non_empty_file" {
  triggers = {
    always_run = formatdate("YYYY-MM-DD-hh-mm-ss", timestamp())
  }

  provisioner "local-exec" {
    command = "bq query --use_legacy_sql=false 'SELECT * FROM ${google_bigquery_dataset.dataset.project}.${google_bigquery_dataset.dataset.dataset_id}.${google_bigquery_table.biglake_with_explicit_schema_manual_metadata_refresh.table_id} WHERE year is not null'"
  }

  depends_on = [
    google_storage_bucket_object.non_empty_parquet_file,
    null_resource.refresh_metadata_cache
  ]
}

resource "google_bigquery_table" "external_parquet_table_with_explicit_schema" {
  project             = var.project
  dataset_id          = google_bigquery_dataset.dataset.dataset_id
  table_id            = "${local.external_parquet_table}_with_explicit_schema"
  deletion_protection = false
  schema              = file(local.bq_schema_file)
  # schema = null
  # max_staleness = "0-0 7 0:0:0" # 7 days


  external_data_configuration {
    autodetect    = false
    source_format = "PARQUET"
    # metadata_cache_mode = "MANUAL" # CALL BQ.REFRESH_EXTERNAL_METADATA_CACHE('myproject.test_db.test_table')
    # metadata_cache_mode = "AUTOMATIC" # requires max_staleness #  googleapi: Error 400: maxStaleness must be specified when MetadataCacheMode is AUTOMATIC, invalid
    # connection_id = "${var.bq_project}.us.${data.external.bq_connection_id.result.connectionId}"

    parquet_options {
      enable_list_inference = true
    }

    hive_partitioning_options {
      mode              = "CUSTOM"
      source_uri_prefix = "gs://${google_storage_bucket.bucket.name}/${local.gcs_parquet_files_root_path}/{year:STRING}/{month:STRING}/{day:STRING}/{hour:STRING}"
    }

    source_uris = ["gs://${google_storage_bucket.bucket.name}/${local.gcs_parquet_files_root_path}/*.parquet"]
  }

  depends_on = [
    google_storage_bucket_object.empty_parquet_file
  ]
}

resource "null_resource" "bq_show_schema_external_parquet_table_with_explicit_schema" {
  triggers = {
    always_run = formatdate("YYYY-MM-DD-hh-mm-ss", timestamp())
  }

  provisioner "local-exec" {
    command = "bq show --format=prettyjson ${google_bigquery_dataset.dataset.project}:${google_bigquery_dataset.dataset.dataset_id}.${google_bigquery_table.external_parquet_table_with_explicit_schema.table_id} | jq '.schema.fields' | jq '.[] | select(.name==\"myRequiredDateTime\")'"
  }
}

resource "null_resource" "bq_select_external_parquet_table_with_explicit_schema_with_empty_file_only" {
  triggers = {
    always_run = formatdate("YYYY-MM-DD-hh-mm-ss", timestamp())
  }

  provisioner "local-exec" {
    command = "bq query --use_legacy_sql=false 'SELECT count(*) AS cnt FROM ${google_bigquery_dataset.dataset.project}.${google_bigquery_dataset.dataset.dataset_id}.${google_bigquery_table.external_parquet_table_with_explicit_schema.table_id} WHERE year is not null'"
  }
}

resource "null_resource" "bq_select_external_parquet_table_explicit_schema_with_non_empty_file" {
  triggers = {
    always_run = formatdate("YYYY-MM-DD-hh-mm-ss", timestamp())
  }

  provisioner "local-exec" {
    command = "bq query --use_legacy_sql=false 'SELECT * FROM ${google_bigquery_dataset.dataset.project}.${google_bigquery_dataset.dataset.dataset_id}.${google_bigquery_table.external_parquet_table_with_explicit_schema.table_id} WHERE year is not null'"
  }

  depends_on = [google_storage_bucket_object.non_empty_parquet_file]
}


########
# AVRO #
########

resource "google_bigquery_table" "external_avro_table_with_explicit_schema" {
  project             = var.project
  dataset_id          = google_bigquery_dataset.dataset.dataset_id
  table_id            = "${local.external_avro_table}_with_explicit_schema"
  deletion_protection = false
  schema = file(local.bq_schema_file)

  external_data_configuration {
    autodetect    = false
    source_format = "AVRO"

    avro_options {
      use_avro_logical_types = true
    }

    hive_partitioning_options {
      mode              = "CUSTOM"
      source_uri_prefix = "gs://${google_storage_bucket.bucket.name}/${local.gcs_avro_files_root_path}/{year:STRING}/{month:STRING}/{day:STRING}/{hour:STRING}"
    }

    source_uris = ["gs://${google_storage_bucket.bucket.name}/${local.gcs_avro_files_root_path}/*.avro"]
  }

  depends_on = [
    // google_storage_bucket_object.empty_avro_file # not required as neither view is created on top of this table nor it has metadata cache refresh enabled
  ]
}

resource "null_resource" "bq_show_schema_external_avro_table_with_explicit_schema" {
  triggers = {
    always_run = formatdate("YYYY-MM-DD-hh-mm-ss", timestamp())
  }

  provisioner "local-exec" {
    command = "bq show --format=prettyjson ${google_bigquery_dataset.dataset.project}:${google_bigquery_dataset.dataset.dataset_id}.${google_bigquery_table.external_avro_table_with_explicit_schema.table_id} | jq '.schema.fields' | jq '.[] | select(.name==\"myRequiredDateTime\")'"
  }
}

resource "google_storage_bucket_object" "empty_avro_file" {
  name   = "${local.gcs_avro_files_root_path}/${local.gcs_hive_zero_partition_path}/${local.empty_avro_file}"
  source = local.empty_avro_file
  bucket = google_storage_bucket.bucket.name
}

resource "null_resource" "bq_select_external_avro_table_with_explicit_schema_with_empty_file_only" {
  triggers = {
    always_run = formatdate("YYYY-MM-DD-hh-mm-ss", timestamp())
  }

  provisioner "local-exec" {
    command = "bq query --use_legacy_sql=false 'SELECT count(*) AS cnt FROM ${google_bigquery_dataset.dataset.project}.${google_bigquery_dataset.dataset.dataset_id}.${google_bigquery_table.external_avro_table_with_explicit_schema.table_id} WHERE year is not null'"
  }

  depends_on = [
    google_storage_bucket_object.empty_avro_file
  ]
}

resource "google_storage_bucket_object" "non_empty_avro_file" {
  name       = "${local.gcs_avro_files_root_path}/${local.gcs_hive_non_zero_partition_path}/${local.non_empty_avro_file}"
  source     = local.non_empty_avro_file
  bucket     = google_storage_bucket.bucket.name
  depends_on = [
    null_resource.bq_select_external_avro_table_with_explicit_schema_with_empty_file_only
  ]
}

resource "null_resource" "bq_select_external_avro_table_explicit_schema_with_non_empty_file" {
  triggers = {
    always_run = formatdate("YYYY-MM-DD-hh-mm-ss", timestamp())
  }

  provisioner "local-exec" {
    command = "bq query --use_legacy_sql=false 'SELECT * FROM ${google_bigquery_dataset.dataset.project}.${google_bigquery_dataset.dataset.dataset_id}.${google_bigquery_table.external_avro_table_with_explicit_schema.table_id} WHERE year is not null'"
  }

  depends_on = [google_storage_bucket_object.non_empty_avro_file]
}

# bq mkdef \
# --connection_id=${GCP_BQ_PROJECT}.us.my_connection \
# --source_format=PARQUET \
# --parquet_enable_list_inference \
# --hive_partitioning_mode=CUSTOM \
# --hive_partitioning_source_uri_prefix="gs://${GCP_BUCKET}/parquet/year:STRING}/{month:STRING}/{day:STRING}/{hour:STRING}" \
# --require_hive_partition_filter=true \
# --metadata_cache_mode=MANUAL \
# "gs://${GCP_BUCKET}/parquet/*.parquet" >  biglake_table_with_manual_metadata_refresh_def
#
#
# bq mk --table --external_table_definition=biglake_table_with_manual_metadata_refresh_def \
# --max_staleness='0-0 0 1:00:0' \
# ${GCP_BQ_PROJECT}:${GCP_OWNER}_biglake_external.biglake_table_with_manual_metadata_refresh \
# myNestedParquetBigQuerySchema.json
#
#
# curl -X POST \
#  -H "Authorization: Bearer $(gcloud auth print-access-token)" \
#  -H "Content-Type: application/json" \
#  -d '{
#    "tableReference": {
#      "projectId": "${GCP_BQ_PROJECT}",
#      "datasetId": "${GCP_OWNER}_biglake_external",
#      "tableId": "biglake_table_with_manual_metadata_refresh"
#    },
#    "externalDataConfiguration": {
# 	   "sourceUris": ["gs://${GCP_BUCKET}/parquet/*.parquet"],
# 	   "sourceFormat": "PARQUET",
# 	   "autodetect": true,
# 	   "hivePartitioningOptions": {
# 	     "mode": "CUSTOM",
#  	     "sourceUriPrefix": "gs://${GCP_BUCKET}/parquet/{year:STRING}/{month:STRING}/{day:STRING}/{hour:STRING}",
#        "requirePartitionFilter": true,
#      },
# 	   "connectionId": "${GCP_BQ_PROJECT}.us.my_connection"
#    }
#  }' \
# "https://bigquery.googleapis.com/bigquery/v2/projects/${GCP_BQ_PROJECT}/datasets/${GCP_OWNER}_biglake_external/tables"
#
#
# schema="`cat myNestedParquetBigQuerySchema.json`"
# curl -X POST \
#  -H "Authorization: Bearer $(gcloud auth print-access-token)" \
#  -H "Content-Type: application/json" \
#  -d "{
#    \"tableReference\": {
#      \"projectId\": \"${GCP_BQ_PROJECT}\",
#      \"datasetId\": \"${GCP_OWNER}_biglake_external\",
#      \"tableId\": \"biglake_table_with_manual_metadata_refresh\"
#    },
#    \"schema\": {
# 	\"fields\": $schema
#    },
#    \"externalDataConfiguration\": {
# 	\"sourceUris\": [\"gs://${GCP_BQ_PROJECT}/parquet/*.parquet\"],
# 	\"sourceFormat\": \"PARQUET\",
# 	\"autodetect\": false,
# 	\"hivePartitioningOptions\": {
# 	  \"mode\": \"CUSTOM\",
#  	  \"sourceUriPrefix\": \"gs://${GCP_BQ_PROJECT}/parquet/{year:STRING}/{month:STRING}/{day:STRING}/{hour:STRING}\",
#         \"requirePartitionFilter\": true,
#       },
# 	\"connectionId\": \"{GCP_BQ_PROJECT}.us.my_connection\",
#    }
#  }" \
# https://bigquery.googleapis.com/bigquery/v2/projects/${GCP_BQ_PROJECT}/datasets/${GCP_OWNER}_biglake_external/tables
