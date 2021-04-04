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
SUBNETWORK=$(curl http://metadata.google.internal/computeMetadata/v1/instance/attributes/subnetwork -H "Metadata-Flavor: Google")
echo "SUBNETWORK=$SUBNETWORK" | tee -a ${LOG}
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
WAIT_SECS_BEFORE_VM_DELETE=$(curl http://metadata.google.internal/computeMetadata/v1/instance/attributes/wait_secs_before_delete -H "Metadata-Flavor: Google")
echo "WAIT_SECS_BEFORE_VM_DELETE=$WAIT_SECS_BEFORE_VM_DELETE" | tee -a ${LOG}

echo "Installing java" | tee -a ${LOG}
max_retry=10; counter=1; until which java ; do sleep $((counter*10)); [[ counter -eq $max_retry ]] && echo "Failed" && break; echo "Trying to install java-1.8.0-openjdk-devel: $counter attempt" | tee -a ${LOG} ; yum install java-1.8.0-openjdk-devel -y 2>&1 | tee -a ${LOG} ; ((counter++)); done
java -version 2>&1 | tee -a ${LOG}
gsutil cp ${DATAFLOW_JAR_GCS_PATH} . 2>&1 | tee -a ${LOG}
JAVA_DATAFLOW_RUN_OPTS="--project=$PROJECT --region=$REGION --serviceAccount=$SERVICE_ACCOUNT --subnetwork=$SUBNETWORK --usePublicIps=false"
echo "Creating template $DATAFLOW_TEMPLATE_GCS_PATH" | tee -a ${LOG}
java -Dorg.xerial.snappy.tempdir=$(pwd) -cp ${DATAFLOW_JAR} com.bawi.beam.dataflow.MyBQReadWriteJob \
  ${JAVA_DATAFLOW_RUN_OPTS} \
  --runner=DataflowRunner \
  --stagingLocation=gs://${BUCKET}/staging \
  --templateLocation=${DATAFLOW_TEMPLATE_GCS_PATH} 2>&1 | tee -a ${LOG}
echo "Done" | tee -a ${LOG}
echo "Uploading log file and deleting instance in $WAIT_SECS_BEFORE_VM_DELETE secs: gcloud compute instances delete $INSTANCE --zone=$ZONE --quiet" | tee -a ${LOG}
gsutil cp ${LOG} gs://${BUCKET}/compute/
sleep ${WAIT_SECS_BEFORE_VM_DELETE}
gcloud compute instances delete ${INSTANCE} --zone=${ZONE} --quiet
