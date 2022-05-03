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
WAIT_SECS_BEFORE_VM_DELETE=$(curl http://metadata.google.internal/computeMetadata/v1/instance/attributes/wait_secs_before_delete -H "Metadata-Flavor: Google")
echo "WAIT_SECS_BEFORE_VM_DELETE=$WAIT_SECS_BEFORE_VM_DELETE" | tee -a ${LOG}

#echo "Installing java" | tee -a ${LOG}
#sudo yum-config-manager --enable rhui-rhel*
#max_retry=10; counter=1; until which java ; do sleep $((counter*10)); [[ counter -eq $max_retry ]] && echo "Failed" && break; echo "Trying to install java-11-openjdk-devel: $counter attempt" | tee -a ${LOG} ; sudo yum install java-11-openjdk-devel -y 2>&1 | tee -a ${LOG} ; ((counter++)); done

gcloud compute instances add-metadata --zone ${ZONE} ${INSTANCE} --metadata=startup-state="(1/3) Checking Java ..."

echo "Removing existing openjdk installation:" | tee -a ${LOG}
rpm -qa | grep openjdk | xargs sudo yum -y remove

openjdkVersion=17.0.2
echo "Installing openjdk ${openjdkVersion}:" | tee -a ${LOG}

#sudo yum-config-manager --enable rhui-rhel*
#sudo yum update -y

gsutil cp gs://${PROJECT}-${OWNER}/openjdk-${openjdkVersion}_linux-x64_bin.tar.gz .
tar xzf openjdk-${openjdkVersion}_linux-x64_bin.tar.gz
sudo mv jdk-${openjdkVersion} /opt/
export JAVA_HOME=/opt/jdk-${openjdkVersion}
export PATH=$JAVA_HOME/bin:$PATH

max_retry=10
counter=1
until which java
do sleep $((counter*10))
  [[ counter -eq $max_retry ]] && echo "Java OpenJDK installation status: failed" &&  gcloud compute instances add-metadata --zone ${ZONE} ${INSTANCE} --metadata=startup-state="(2/3) Java OpenJDK installation status: failed" && exit 1
  echo "Trying to install java-11-openjdk-devel: $counter attempt"
  sudo yum install java-11-openjdk-devel -y 2>&1
  ((counter++))
done

which java
java -version
java -version 2>&1 | tee -a ${LOG}
echo "Java OpenJDK installation status: completed"

gcloud compute instances add-metadata --zone ${ZONE} ${INSTANCE} --metadata=startup-state="(3/3) Creating dataflow template"

gsutil cp ${DATAFLOW_JAR_GCS_PATH} . 2>&1 | tee -a ${LOG}
JAVA_DATAFLOW_RUN_OPTS="--project=$PROJECT --region=$REGION --serviceAccount=$SERVICE_ACCOUNT --usePublicIps=false"
echo "Creating template $DATAFLOW_TEMPLATE_GCS_PATH" | tee -a ${LOG}
java -Dorg.xerial.snappy.tempdir="$(pwd)" -cp ${DATAFLOW_JAR} ${DATAFLOW_JAR_MAIN_CLASS} \
  ${JAVA_DATAFLOW_RUN_OPTS} \
  --runner=DataflowRunner \
  --stagingLocation=gs://${BUCKET}/staging \
  --templateLocation=${DATAFLOW_TEMPLATE_GCS_PATH} 2>&1 | tee -a ${LOG}

echo "Done" | tee -a ${LOG}
echo "Uploading log file and deleting instance in $WAIT_SECS_BEFORE_VM_DELETE secs: gcloud compute instances delete $INSTANCE --zone=$ZONE --quiet" | tee -a ${LOG}
gsutil cp ${LOG} gs://${BUCKET}/compute/
sleep ${WAIT_SECS_BEFORE_VM_DELETE}
gcloud compute instances delete ${INSTANCE} --zone=${ZONE} --quiet
