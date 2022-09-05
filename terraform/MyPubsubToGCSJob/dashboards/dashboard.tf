resource "google_monitoring_dashboard" "dashboard_job_id" {
  project = var.project
  dashboard_json = templatefile("${path.module}/${var.dashboard_file}", {
    //metric.label."job_id"="2021-05-21_08_48_27-16638284577175562727"
//    dataflow_job_filter = "metric.label.\\\"job_id\\\"=\\\"${google_dataflow_flex_template_job.my_dataflow_flex_job.id}\\\""
    dataflow_job_filter = "metric.label.\\\"job_id\\\"=\\\"${var.job_id}\\\""

    //metadata.user_labels."dataflow_job_id"="2021-05-21_08_48_27-16638284577175562727"
//    instance_dataflow_job_filter = "metadata.user_labels.\\\"dataflow_job_id\\\"=\\\"${google_dataflow_flex_template_job.my_dataflow_flex_job.id}\\\""
    instance_dataflow_job_filter = "metadata.user_labels.\\\"dataflow_job_id\\\"=\\\"${var.job_id}\\\""
    dashboard_name = "${var.job_name} job id"

    //transform_step_name = "MapElements/Map.out0"
    read_step_name = "PubsubIO.Read"
    read_step_pcollection = "PubsubIO.Read/PubsubUnboundedSource.out0"

    transform_step_name = "ConcatBodyAttrAndMsgIdFn"
    transform_step_pcollection = "ConcatBodyAttrAndMsgIdFn.out0"

    write_step_name = "AvroIO.Write"
    write_step_pcollection = "AvroIO.Write/AvroIO.TypedWrite/Write/WriteShardedBundlesToTempFiles/ApplyShardingKey.out0"
//    write_step_name = "FileIO.Write"
//    write_step_pcollection = "FileIO.Write/WriteFiles/WriteShardedBundlesToTempFiles/ApplyShardingKey.out0"

    topic = var.topic
    subscription = var.subscription
  })
}

resource "google_monitoring_dashboard" "dashboard_job_name_current" {
  project = var.project
  dashboard_json = templatefile("${path.module}/${var.dashboard_file}", {
    dataflow_job_filter = "resource.label.\\\"job_name\\\"=\\\"${var.job_name}\\\""
    instance_dataflow_job_filter = "metadata.user_labels.\\\"dataflow_job_name\\\"=\\\"${var.job_name}\\\""
    dashboard_name = "${var.job_name} job name current"
    read_step_name = "PubsubIO.Read"
    read_step_pcollection = "PubsubIO.Read/PubsubUnboundedSource.out0"
    transform_step_name = "ConcatBodyAttrAndMsgIdFn"
    transform_step_pcollection = "ConcatBodyAttrAndMsgIdFn.out0"
    write_step_name = "AvroIO.Write"
    write_step_pcollection = "AvroIO.Write/AvroIO.TypedWrite/Write/WriteShardedBundlesToTempFiles/ApplyShardingKey.out0"
//    write_step_name = "FileIO.Write"
//    write_step_pcollection = "FileIO.Write/WriteFiles/WriteShardedBundlesToTempFiles/ApplyShardingKey.out0"
    topic = var.topic
    subscription = var.subscription
  })
}

resource "google_monitoring_dashboard" "dashboard_job_name_all" {
  project = var.project
  dashboard_json = templatefile("${path.module}/${var.dashboard_file}", {
    dataflow_job_filter = "resource.label.\\\"job_name\\\"=monitoring.regex.full_match(\\\"${var.job_base_name}.*\\\")"
    instance_dataflow_job_filter = "metadata.user_labels.\\\"dataflow_job_name\\\"=monitoring.regex.full_match(\\\"${var.job_base_name}.*\\\")"
    dashboard_name = "${var.job_base_name}.* job name all"
    read_step_name = "PubsubIO.Read"
    read_step_pcollection = "PubsubIO.Read/PubsubUnboundedSource.out0"
    transform_step_name = "ConcatBodyAttrAndMsgIdFn"
    transform_step_pcollection = "ConcatBodyAttrAndMsgIdFn.out0"
    write_step_name = "AvroIO.Write"
    write_step_pcollection = "AvroIO.Write/AvroIO.TypedWrite/Write/WriteShardedBundlesToTempFiles/ApplyShardingKey.out0"
    //    write_step_name = "FileIO.Write"
    //    write_step_pcollection = "FileIO.Write/WriteFiles/WriteShardedBundlesToTempFiles/ApplyShardingKey.out0"
    topic = var.topic
    subscription = var.subscription
  })
}

//resource "google_monitoring_dashboard" "dashboard_redesigned_job_id" {
//  project = var.project
//  dashboard_json = templatefile("${path.module}/dashboard_streaming_redesigned.json", {
//    //metric.label."job_id"="2021-05-21_08_48_27-16638284577175562727"
////    dataflow_job_filter = "metric.label.\\\"job_id\\\"=\\\"${google_dataflow_flex_template_job.my_dataflow_flex_job.id}\\\""
//    dataflow_job_filter = "metric.label.\\\"job_id\\\"=\\\"${var.job_id}\\\""
//
//    //metadata.user_labels."dataflow_job_id"="2021-05-21_08_48_27-16638284577175562727"
////    instance_dataflow_job_filter = "metadata.user_labels.\\\"dataflow_job_id\\\"=\\\"${google_dataflow_flex_template_job.my_dataflow_flex_job.id}\\\""
//    instance_dataflow_job_filter = "metadata.user_labels.\\\"dataflow_job_id\\\"=\\\"${var.job_id}\\\""
//    dashboard_name = "${var.job_name} redesigned job id"
//
//    //transform_step_name = "MapElements/Map.out0"
//    read_step_name = "PubsubIO.Read"
//    read_step_pcollection = "PubsubIO.Read/PubsubUnboundedSource.out0"
//
//    transform_step_name = "ConcatBodyAttrAndMsgIdFn"
//    transform_step_pcollection = "ConcatBodyAttrAndMsgIdFn.out0"
//
//    write_step_name = "AvroIO.Write"
//    write_step_pcollection = "AvroIO.Write/AvroIO.TypedWrite/Write/WriteShardedBundlesToTempFiles/ApplyShardingKey.out0"
////    write_step_name = "FileIO.Write"
////    write_step_pcollection = "FileIO.Write/WriteFiles/WriteShardedBundlesToTempFiles/ApplyShardingKey.out0"
//
//    topic = var.topic
//    subscription = var.subscription
//  })
//}
//
//
//resource "google_monitoring_dashboard" "dashboard_redesigned_job_name_current" {
//  project = var.project
//  dashboard_json = templatefile("${path.module}/dashboard_streaming_redesigned.json", {
//    dataflow_job_filter = "resource.label.\\\"job_name\\\"=\\\"${var.job_name}\\\""
//    instance_dataflow_job_filter = "metadata.user_labels.\\\"dataflow_job_name\\\"=\\\"${var.job_name}\\\""
//    dashboard_name = "${var.job_name} redesigned job name"
//    read_step_name = "PubsubIO.Read"
//    read_step_pcollection = "PubsubIO.Read/PubsubUnboundedSource.out0"
//    transform_step_name = "ConcatBodyAttrAndMsgIdFn"
//    transform_step_pcollection = "ConcatBodyAttrAndMsgIdFn.out0"
//    write_step_name = "AvroIO.Write"
//    write_step_pcollection = "AvroIO.Write/AvroIO.TypedWrite/Write/WriteShardedBundlesToTempFiles/ApplyShardingKey.out0"
////    write_step_name = "FileIO.Write"
////    write_step_pcollection = "FileIO.Write/WriteFiles/WriteShardedBundlesToTempFiles/ApplyShardingKey.out0"
//    topic = var.topic
//    subscription = var.subscription
//  })
//}
//
//resource "google_monitoring_dashboard" "dashboard_redesigned_job_name_all" {
//  project = var.project
//  dashboard_json = templatefile("${path.module}/dashboard_streaming_redesigned.json", {
//    dataflow_job_filter = "resource.label.\\\"job_name\\\"=monitoring.regex.full_match(\\\"${var.job}.*\\\")"
//    instance_dataflow_job_filter = "metadata.user_labels.\\\"dataflow_job_name\\\"=monitoring.regex.full_match(\\\"${var.job}.*\\\")"
//    dashboard_name = "${var.job}.* redesigned job name all"
//    read_step_name = "PubsubIO.Read"
//    read_step_pcollection = "PubsubIO.Read/PubsubUnboundedSource.out0"
//    transform_step_name = "ConcatBodyAttrAndMsgIdFn"
//    transform_step_pcollection = "ConcatBodyAttrAndMsgIdFn.out0"
//    write_step_name = "AvroIO.Write"
//    write_step_pcollection = "AvroIO.Write/AvroIO.TypedWrite/Write/WriteShardedBundlesToTempFiles/ApplyShardingKey.out0"
//    //    write_step_name = "FileIO.Write"
//    //    write_step_pcollection = "FileIO.Write/WriteFiles/WriteShardedBundlesToTempFiles/ApplyShardingKey.out0"
//    topic = var.topic
//    subscription = var.subscription
//  })
//}
