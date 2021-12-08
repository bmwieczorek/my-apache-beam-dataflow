resource "google_monitoring_dashboard" "dashboard_job_id" {
  project = var.project
  dashboard_json = templatefile(var.dashboard_file, {
    job = local.job
    //metric.label."job_id"="2021-05-21_08_48_27-16638284577175562727"
    dataflow_job_filter = "metric.label.\\\"job_id\\\"=\\\"${google_dataflow_flex_template_job.my_dataflow_flex_job.id}\\\""

    //metadata.user_labels."dataflow_job_id"="2021-05-21_08_48_27-16638284577175562727"
    instance_dataflow_job_filter = "metadata.user_labels.\\\"dataflow_job_id\\\"=\\\"${google_dataflow_flex_template_job.my_dataflow_flex_job.id}\\\""
    dashboard_name = "${local.job} job id"

    //transform_step_name = "MapElements/Map.out0"
    read_step_name = "PubsubIO.Read"
    read_step_pcollection = "PubsubIO.Read/PubsubUnboundedSource.out0"

    transform_step_name = "ConcatBodyAttrAndMsgIdFn"
    transform_step_pcollection = "ConcatBodyAttrAndMsgIdFn.out0"

    write_step_name = "AvroIO.Write"
    write_step_pcollection = "AvroIO.Write/AvroIO.TypedWrite/Write/WriteShardedBundlesToTempFiles/ApplyShardingKey.out0"

    topic = local.topic
    subscription = local.subscription
  })
}

resource "google_monitoring_dashboard" "dashboard_job_name" {
  project = var.project
  dashboard_json = templatefile(var.dashboard_file, {
    job = local.job
    dataflow_job_filter = "resource.label.\\\"job_name\\\"=\\\"${local.job}\\\""
    instance_dataflow_job_filter = "metadata.user_labels.\\\"dataflow_job_name\\\"=\\\"${local.job}\\\""
    dashboard_name = "${local.job} job name"
    read_step_name = "PubsubIO.Read"
    read_step_pcollection = "PubsubIO.Read/PubsubUnboundedSource.out0"
    transform_step_name = "ConcatBodyAttrAndMsgIdFn"
    transform_step_pcollection = "ConcatBodyAttrAndMsgIdFn.out0"
    write_step_name = "AvroIO.Write"
    write_step_pcollection = "AvroIO.Write/AvroIO.TypedWrite/Write/WriteShardedBundlesToTempFiles/ApplyShardingKey.out0"
    topic = local.topic
    subscription = local.subscription
  })
}

resource "google_monitoring_dashboard" "dashboard_redesigned_job_id" {
  project = var.project
  dashboard_json = templatefile("dashboard_streaming_redesigned.json", {
    job = local.job
    //metric.label."job_id"="2021-05-21_08_48_27-16638284577175562727"
    dataflow_job_filter = "metric.label.\\\"job_id\\\"=\\\"${google_dataflow_flex_template_job.my_dataflow_flex_job.id}\\\""

    //metadata.user_labels."dataflow_job_id"="2021-05-21_08_48_27-16638284577175562727"
    instance_dataflow_job_filter = "metadata.user_labels.\\\"dataflow_job_id\\\"=\\\"${google_dataflow_flex_template_job.my_dataflow_flex_job.id}\\\""
    dashboard_name = "${local.job} redesigned job id"

    //transform_step_name = "MapElements/Map.out0"
    read_step_name = "PubsubIO.Read"
    read_step_pcollection = "PubsubIO.Read/PubsubUnboundedSource.out0"

    transform_step_name = "ConcatBodyAttrAndMsgIdFn"
    transform_step_pcollection = "ConcatBodyAttrAndMsgIdFn.out0"

    write_step_name = "AvroIO.Write"
    write_step_pcollection = "AvroIO.Write/AvroIO.TypedWrite/Write/WriteShardedBundlesToTempFiles/ApplyShardingKey.out0"

    topic = local.topic
    subscription = local.subscription
  })
}


resource "google_monitoring_dashboard" "dashboard_redesigned_job_name" {
  project = var.project
  dashboard_json = templatefile("dashboard_streaming_redesigned.json", {
    job = local.job
    dataflow_job_filter = "resource.label.\\\"job_name\\\"=\\\"${local.job}\\\""
    instance_dataflow_job_filter = "metadata.user_labels.\\\"dataflow_job_name\\\"=\\\"${local.job}\\\""
    dashboard_name = "${local.job} redesigned job name"
    read_step_name = "PubsubIO.Read"
    read_step_pcollection = "PubsubIO.Read/PubsubUnboundedSource.out0"
    transform_step_name = "ConcatBodyAttrAndMsgIdFn"
    transform_step_pcollection = "ConcatBodyAttrAndMsgIdFn.out0"
    write_step_name = "AvroIO.Write"
    write_step_pcollection = "AvroIO.Write/AvroIO.TypedWrite/Write/WriteShardedBundlesToTempFiles/ApplyShardingKey.out0"
    topic = local.topic
    subscription = local.subscription
  })
}

/*

resource "google_monitoring_dashboard" "airshopping_redesigned_job_name" {
  project = var.project
  dashboard_json = templatefile("dashboard_streaming_redesigned.json", {
    job = "airshopping-data-ingestion"
    dataflow_job_filter = "resource.label.\\\"job_name\\\"=monitoring.regex.full_match(\\\"airshopping-data-ingestion-.*\\\")"
    instance_dataflow_job_filter = "metadata.user_labels.\\\"dataflow_job_name\\\"=monitoring.regex.full_match(\\\"airshopping-data-ingestion-.*\\\")"
    dashboard_name = "airshopping-data-ingestion redesigned job name"
    read_step_name = "ReadPubsubMultiFwdMsg"
    read_step_pcollection = "ReadPubsubMultiFwdMsg/MapElements/Map.out0"
    transform_step_name = "ToGenericShopRecords"
    transform_step_pcollection = "ToGenericShopRecords.out"
    write_step_name = "WriteBQshopRecord"
    write_step_pcollection = "WriteBQshopRecord/PrepareWrite/ParDo(Anonymous).out0"
    topic = "sfw-airshopping"
    subscription = "sfw-airshopping-topic"
  })
}

resource "google_monitoring_dashboard" "airshopping_redesigned_job_id" {
  project = var.project
  dashboard_json = templatefile("dashboard_streaming_redesigned.json", {
    job = "airshopping-data-ingestion"
    dataflow_job_filter = "metric.label.\\\"job_id\\\"=\\\"2021-05-26_09_34_30-17149002215298212728\\\""
    instance_dataflow_job_filter = "metadata.user_labels.\\\"dataflow_job_id\\\"=\\\"2021-05-26_09_34_30-17149002215298212728\\\""
    dashboard_name = "airshopping-data-ingestion redesigned job id 2021-05-26_09_34_30-17149002215298212728"
    read_step_name = "ReadPubsubMultiFwdMsg"
    read_step_pcollection = "ReadPubsubMultiFwdMsg/MapElements/Map.out0"
    transform_step_name = "ToGenericShopRecords"
    transform_step_pcollection = "ToGenericShopRecords.out0"
    write_step_name = "WriteBQshopRecord"
    write_step_pcollection = "WriteBQshopRecord/PrepareWrite/ParDo(Anonymous).out0"
    topic = "sfw-airshopping"
    subscription = "sfw-airshopping-topic"
  })
}

resource "google_monitoring_dashboard" "robotic_redesigned_job_name" {
  project = var.project
  dashboard_json = templatefile("dashboard_batch_redesigned.json", {
    job = "robotic-shopping"
    dataflow_job_filter = "resource.label.\\\"job_name\\\"=monitoring.regex.full_match(\\\"robotic-shopping-.*\\\")"
    instance_dataflow_job_filter = "metadata.user_labels.\\\"dataflow_job_name\\\"=monitoring.regex.full_match(\\\"robotic-shopping-.*\\\")"
    dashboard_name = "robotic-shopping redesigned job name"
    read_step_name = "Read BQ shop_record"
    read_step_pcollection = "Read BQ shop_record/PassThroughThenCleanup/ParMultiDo(Identity).out0"
    transform_step_name = "Calculate robotic ind"
    transform_step_pcollection = "Calculate robotic ind/To GenericRecord.out0"
    write_step_name = "Write BQ robotic_stg"
    write_step_pcollection = "Write BQ robotic_stg/PrepareWrite/ParDo(Anonymous).out0"
  })
}

resource "google_monitoring_dashboard" "robotic_redesigned_job_id" {
  project = var.project
  dashboard_json = templatefile("dashboard_batch_redesigned.json", {
    job = "robotic-shopping"
    dataflow_job_filter = "metric.label.\\\"job_id\\\"=\\\"2021-05-27_05_53_24-8109596674050890303\\\""
    instance_dataflow_job_filter = "metadata.user_labels.\\\"dataflow_job_id\\\"=\\\"2021-05-27_05_53_24-8109596674050890303\\\""
    dashboard_name = "robotic-shopping redesigned job id 2021-05-27_05_53_24-8109596674050890303"
    read_step_name = "Read BQ shop_record"
    read_step_pcollection = "Read BQ shop_record/PassThroughThenCleanup/ParMultiDo(Identity).out0"
    transform_step_name = "Calculate robotic ind"
    transform_step_pcollection = "Calculate robotic ind/To GenericRecord.out0"
    write_step_name = "Write BQ robotic_stg"
    write_step_pcollection = "Write BQ robotic_stg/PrepareWrite/ParDo(Anonymous).out0"
  })
}
*/
