resource "google_monitoring_dashboard" "dashboard_job_id" {
  project = var.project
  dashboard_json = templatefile(var.dashboard_file, {
    job = var.job
    //metric.label."job_id"="2021-05-21_08_48_27-16638284577175562727"
    dataflow_job_filter = "metric.label.\\\"job_id\\\"=\\\"${google_dataflow_flex_template_job.my_dataflow_flex_job.id}\\\""

    //metadata.user_labels."dataflow_job_id"="2021-05-21_08_48_27-16638284577175562727"
    instance_dataflow_job_filter = "metadata.user_labels.\\\"dataflow_job_id\\\"=\\\"${google_dataflow_flex_template_job.my_dataflow_flex_job.id}\\\""
    dashboard_name = "${var.job} job id"

    //transform_step_name = "MapElements/Map.out0"
    read_step_name = "PubsubIO.Read"
    read_step_pcollection = "PubsubIO.Read/PubsubUnboundedSource.out0"

    transform_step_name = "ConcatBodyAttrAndMsgIdFn"
    transform_step_pcollection = "ConcatBodyAttrAndMsgIdFn.out0"

    write_step_name = "AvroIO.Write"
    write_step_pcollection = "AvroIO.Write/AvroIO.TypedWrite/Write/WriteShardedBundlesToTempFiles/ApplyShardingKey.out0"

    topic = var.topic
    subscription = var.subscription
  })
}

resource "google_monitoring_dashboard" "dashboard_job_name" {
  project = var.project
  dashboard_json = templatefile(var.dashboard_file, {
    job = var.job
    dataflow_job_filter = "resource.label.\\\"job_name\\\"=\\\"${var.job}\\\""
    instance_dataflow_job_filter = "metadata.user_labels.\\\"dataflow_job_name\\\"=\\\"${var.job}\\\""
    dashboard_name = "${var.job} job name"
    read_step_name = "PubsubIO.Read"
    read_step_pcollection = "PubsubIO.Read/PubsubUnboundedSource.out0"
    transform_step_name = "ConcatBodyAttrAndMsgIdFn"
    transform_step_pcollection = "ConcatBodyAttrAndMsgIdFn.out0"
    write_step_name = "AvroIO.Write"
    write_step_pcollection = "AvroIO.Write/AvroIO.TypedWrite/Write/WriteShardedBundlesToTempFiles/ApplyShardingKey.out0"
    topic = var.topic
    subscription = var.subscription
  })
}
