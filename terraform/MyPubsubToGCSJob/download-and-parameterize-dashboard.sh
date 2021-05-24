#!/bin/bash
gcloud monitoring dashboards list --filter="'bartek-mypubsubtogcsjob job name'" --format=json \
| sed '1d' | sed '$d' | grep -v '"displayName":' | grep -v '"etag":' \
| sed 's/bartek-topic-sub/${subscription}/' | sed 's/bartek-topic/${topic}/' \
| sed 's/"name": "projects.*"/"displayName": "${dashboard_name}"/' \
| sed 's/metadata.user_labels.\\"dataflow_job_name\\"=\\"[A-Za-z0-9_-]*\\"/${instance_dataflow_job_filter}/g' \
| sed 's/metadata.user_labels.\\"dataflow_job_id\\"=\\"[A-Za-z0-9_-]*\\"/${instance_dataflow_job_filter}/g' \
| sed 's/metric.label.\\"job_id\\"=\\"[A-Za-z0-9_-]*\\"/${dataflow_job_filter}/' \
| sed 's/resource.label.\\"job_name\\"=\\"[A-Za-z0-9_-]*\\"/${dataflow_job_filter}/' \
| sed 's/ConcatBodyAttrAndMsgIdFn.out0/${transform_step_pcollection}/' \
| sed 's/ConcatBodyAttrAndMsgIdFn/${transform_step_name}/' \
| sed 's/AvroIO.Write\/AvroIO.TypedWrite\/Write\/WriteShardedBundlesToTempFiles\/ApplyShardingKey.out0/${write_step_pcollection}/' \
| sed 's/AvroIO.Write/${write_step_name}/' \
| sed 's/PubsubIO.Read\/PubsubUnboundedSource.out0/${read_step_pcollection}/' \
| sed 's/PubsubIO.Read/${read_step_name}/' > dashboard.json
