#!/bin/bash

LOG="/tmp/startup-script.txt"

PROJECT=$(curl http://metadata.google.internal/computeMetadata/v1/instance/attributes/project -H "Metadata-Flavor: Google")
echo "PROJECT=$PROJECT" | tee -a ${LOG}
REGION=$(curl http://metadata.google.internal/computeMetadata/v1/instance/attributes/region -H "Metadata-Flavor: Google")
echo "REGION=$REGION" | tee -a ${LOG}
ZONE=$(curl http://metadata.google.internal/computeMetadata/v1/instance/attributes/zone -H "Metadata-Flavor: Google")
echo "ZONE=$ZONE" | tee -a ${LOG}
SERVICE_ACCOUNT=$(curl http://metadata.google.internal/computeMetadata/v1/instance/attributes/service_account -H "Metadata-Flavor: Google")
echo "SERVICE_ACCOUNT=$SERVICE_ACCOUNT" | tee -a ${LOG}
OWNER=$(curl http://metadata.google.internal/computeMetadata/v1/instance/attributes/owner -H "Metadata-Flavor: Google")
echo "OWNER=$OWNER" | tee -a ${LOG}
INSTANCE=$(curl http://metadata.google.internal/computeMetadata/v1/instance/attributes/instance -H "Metadata-Flavor: Google")
echo "INSTANCE=$INSTANCE" | tee -a ${LOG}
BUCKET=$(curl http://metadata.google.internal/computeMetadata/v1/instance/attributes/bucket -H "Metadata-Flavor: Google")
echo "BUCKET=$BUCKET" | tee -a ${LOG}
DATAFLOW_TEMPLATE_GCS_PATH=$(curl http://metadata.google.internal/computeMetadata/v1/instance/attributes/template_gcs_path -H "Metadata-Flavor: Google")
echo "DATAFLOW_TEMPLATE_GCS_PATH=$DATAFLOW_TEMPLATE_GCS_PATH" | tee -a ${LOG}
DATAFLOW_JAR=$(curl http://metadata.google.internal/computeMetadata/v1/instance/attributes/dataflow_jar -H "Metadata-Flavor: Google")
echo "DATAFLOW_JAR=$DATAFLOW_JAR" | tee -a ${LOG}
DATAFLOW_JAR_GCS_PATH=$(curl http://metadata.google.internal/computeMetadata/v1/instance/attributes/dataflow_jar_gcs_path -H "Metadata-Flavor: Google")
echo "DATAFLOW_JAR_GCS_PATH=$DATAFLOW_JAR_GCS_PATH" | tee -a ${LOG}
DATAFLOW_JAR_MAIN_CLASS=$(curl http://metadata.google.internal/computeMetadata/v1/instance/attributes/dataflow_jar_main_class -H "Metadata-Flavor: Google")
echo "DATAFLOW_JAR_MAIN_CLASS=$DATAFLOW_JAR_MAIN_CLASS" | tee -a ${LOG}
MESSAGE_DEDUPLICATION_ENABLED=$(curl http://metadata.google.internal/computeMetadata/v1/instance/attributes/message_deduplication_enabled -H "Metadata-Flavor: Google")
echo "MESSAGE_DEDUPLICATION_ENABLED=$MESSAGE_DEDUPLICATION_ENABLED" | tee -a ${LOG}
CUSTOM_EVENT_TIME_TIMESTAMP_ATTRIBUTE_ENABLED=$(curl http://metadata.google.internal/computeMetadata/v1/instance/attributes/custom_event_time_timestamp_attribute_enabled -H "Metadata-Flavor: Google")
echo "CUSTOM_EVENT_TIME_TIMESTAMP_ATTRIBUTE_ENABLED=$CUSTOM_EVENT_TIME_TIMESTAMP_ATTRIBUTE_ENABLED" | tee -a ${LOG}
CUSTOM_EVENT_TIME_TIMESTAMP_ATTRIBUTE=$(curl http://metadata.google.internal/computeMetadata/v1/instance/attributes/custom_event_time_timestamp_attribute -H "Metadata-Flavor: Google")
echo "CUSTOM_EVENT_TIME_TIMESTAMP_ATTRIBUTE=$CUSTOM_EVENT_TIME_TIMESTAMP_ATTRIBUTE" | tee -a ${LOG}
WAIT_SECS_BEFORE_VM_DELETE=$(curl http://metadata.google.internal/computeMetadata/v1/instance/attributes/wait_secs_before_delete -H "Metadata-Flavor: Google")
echo "WAIT_SECS_BEFORE_VM_DELETE=$WAIT_SECS_BEFORE_VM_DELETE" | tee -a ${LOG}
NUMBER_OF_WORKER_HARNESS_THREADS=$(curl http://metadata.google.internal/computeMetadata/v1/instance/attributes/number_of_worker_harness_threads -H "Metadata-Flavor: Google")
echo "NUMBER_OF_WORKER_HARNESS_THREADS=$NUMBER_OF_WORKER_HARNESS_THREADS" | tee -a ${LOG}
ENABLE_STREAMING_ENGINE=$(curl http://metadata.google.internal/computeMetadata/v1/instance/attributes/enable_streaming_engine -H "Metadata-Flavor: Google")
echo "ENABLE_STREAMING_ENGINE=$ENABLE_STREAMING_ENGINE" | tee -a ${LOG}
NUM_SHARDS=$(curl http://metadata.google.internal/computeMetadata/v1/instance/attributes/num_shards -H "Metadata-Flavor: Google")
echo "NUM_SHARDS=NUM_SHARDS" | tee -a ${LOG}
DUMP_HEAP_ON_OOM=$(curl http://metadata.google.internal/computeMetadata/v1/instance/attributes/dump_heap_on_oom -H "Metadata-Flavor: Google")
echo "DUMP_HEAP_ON_OOM=$DUMP_HEAP_ON_OOM" | tee -a ${LOG}

#echo "Installing java" | tee -a ${LOG}
#sudo yum-config-manager --enable rhui-rhel*
#max_retry=10; counter=1; until which java ; do sleep $((counter*10)); [[ counter -eq $max_retry ]] && echo "Failed" && break; echo "Trying to install java-11-openjdk-devel: $counter attempt" | tee -a ${LOG} ; sudo yum install java-11-openjdk-devel -y 2>&1 | tee -a ${LOG} ; ((counter++)); done

gcloud compute instances add-metadata --zone ${ZONE} ${INSTANCE} --metadata=startup-state="(1/3) Checking Java ..."

echo "Checking Java ... "
which java | tee -a ${LOG}
java -version 2>&1 | tee -a ${LOG}

#echo "Removing existing openjdk installation:" | tee -a ${LOG}
#rpm -qa | grep openjdk | xargs sudo yum -y remove
#
#openjdkVersion=17.0.2
#echo "Installing openjdk ${openjdkVersion}:" | tee -a ${LOG}

#sudo yum-config-manager --enable rhui-rhel*
#sudo yum update -y

#gsutil cp gs://${PROJECT}-${OWNER}/openjdk-${openjdkVersion}_linux-x64_bin.tar.gz .
#tar xzf openjdk-${openjdkVersion}_linux-x64_bin.tar.gz
#sudo mv jdk-${openjdkVersion} /opt/
#export JAVA_HOME=/opt/jdk-${openjdkVersion}
#export PATH=$JAVA_HOME/bin:$PATH

#max_retry=10
#counter=1
#until which java
#do sleep $((counter*10))
#  [[ counter -eq $max_retry ]] && echo "Java OpenJDK installation status: failed" &&  gcloud compute instances add-metadata --zone ${ZONE} ${INSTANCE} --metadata=startup-state="(2/3) Java OpenJDK installation status: failed" && exit 1
#  echo "Trying to install java-11-openjdk-devel: $counter attempt"
#  sudo yum install java-11-openjdk-devel -y 2>&1
#  ((counter++))
#done

which java | tee -a ${LOG}
java -version 2>&1 | tee -a ${LOG}
echo "Java OpenJDK installation status: completed"

gcloud compute instances add-metadata --zone ${ZONE} ${INSTANCE} --metadata=startup-state="(3/3) Creating dataflow template"

gsutil -o GSUtil:check_hashes=never cp "${DATAFLOW_JAR_GCS_PATH}" . 2>&1 | tee -a ${LOG}
JAVA_DATAFLOW_RUN_OPTS="--project=$PROJECT --region=$REGION --serviceAccount=$SERVICE_ACCOUNT --usePublicIps=false"
echo "Creating template $DATAFLOW_TEMPLATE_GCS_PATH" | tee -a ${LOG}

echo "Executing: java -Dorg.xerial.snappy.tempdir=$(pwd) -cp ${DATAFLOW_JAR} ${DATAFLOW_JAR_MAIN_CLASS} ${JAVA_DATAFLOW_RUN_OPTS} --runner=DataflowRunner --messageDeduplicationEnabled=${MESSAGE_DEDUPLICATION_ENABLED} --customEventTimeTimestampAttributeEnabled=${CUSTOM_EVENT_TIME_TIMESTAMP_ATTRIBUTE_ENABLED} --timestampAttribute=${CUSTOM_EVENT_TIME_TIMESTAMP_ATTRIBUTE} --stagingLocation=gs://${BUCKET}/staging --dumpHeapOnOOM=${DUMP_HEAP_ON_OOM} --saveHeapDumpsToGcsPath=gs://${BUCKET}/oom --numberOfWorkerHarnessThreads=${NUMBER_OF_WORKER_HARNESS_THREADS} --numWorkers=1 --diskSizeGb=200 --windowSecs=60 --autoscalingAlgorithm=THROUGHPUT_BASED --enableStreamingEngine=${ENABLE_STREAMING_ENGINE} --numShards=${NUM_SHARDS} --templateLocation=${DATAFLOW_TEMPLATE_GCS_PATH}" | tee -a ${LOG}

java -Dorg.xerial.snappy.tempdir="$(pwd)" -cp ${DATAFLOW_JAR} ${DATAFLOW_JAR_MAIN_CLASS} \
  ${JAVA_DATAFLOW_RUN_OPTS} \
  --runner=DataflowRunner \
  --messageDeduplicationEnabled=${MESSAGE_DEDUPLICATION_ENABLED} \
  --customEventTimeTimestampAttributeEnabled=${CUSTOM_EVENT_TIME_TIMESTAMP_ATTRIBUTE_ENABLED} \
  --timestampAttribute=${CUSTOM_EVENT_TIME_TIMESTAMP_ATTRIBUTE} \
  --stagingLocation="gs://${BUCKET}/staging" \
  --dumpHeapOnOOM=${DUMP_HEAP_ON_OOM} \
  --saveHeapDumpsToGcsPath="gs://${BUCKET}/oom" \
  --numberOfWorkerHarnessThreads=${NUMBER_OF_WORKER_HARNESS_THREADS} \
  --numWorkers=1 \
  --diskSizeGb=200 \
  --windowSecs=60 \
  --autoscalingAlgorithm=THROUGHPUT_BASED \
  --enableStreamingEngine=${ENABLE_STREAMING_ENGINE} \
  --numShards=${NUM_SHARDS} \
  --templateLocation="${DATAFLOW_TEMPLATE_GCS_PATH}" 2>&1 | tee -a ${LOG}

# streaming engine is required for auto-sharding
#  --enableStreamingEngine=${ENABLE_STREAMING_ENGINE} \  // passed to dataflow_classic_template_job when starting a job (it is also possible at template generation)

#  --workerLogLevelOverrides="{ \"org.apache.beam.sdk.util.WindowTracing\": \"DEBUG\" }" \

#  --gcpTempLocation="gs://${BUCKET}/gcpTempLocation" \

#  --workerDiskType="pd-standard" \
#  --streaming=true \
#  --profilingAgentConfiguration="{ \"APICurated\" : true }" \

#  static pipeline options only can be set in while template generation --windowSecs=60 IntervalWindow: [2025-03-25T17:12:00.000Z..2025-03-25T17:13:00.000Z); note: value set asa param in resource "google_dataflow_job" "job" is not used

# add maxNumWorkers here to determine number of consumers for KafkaIO read
#  --maxNumWorkers=2 \
result=$?

echo "Uploading log file and deleting instance in $WAIT_SECS_BEFORE_VM_DELETE secs: gcloud compute instances delete $INSTANCE --zone=$ZONE --quiet" | tee -a ${LOG}
gsutil cp ${LOG} gs://${BUCKET}/compute/

echo "Done" | tee -a ${LOG}

if [ ${result} -ne 0 ]; then
  grep 'Exception in thread "main"' ${LOG} | tee -a ${LOG}
  gcloud compute instances add-metadata --zone ${ZONE} ${INSTANCE} --metadata=startup-state="Failed"
else
  gcloud compute instances add-metadata --zone ${ZONE} ${INSTANCE} --metadata=startup-state="Completed"
fi

sleep ${WAIT_SECS_BEFORE_VM_DELETE}
gcloud compute instances delete ${INSTANCE} --zone=${ZONE} --quiet
