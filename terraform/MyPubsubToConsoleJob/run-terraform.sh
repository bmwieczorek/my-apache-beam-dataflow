#!/bin/bash

function run_terraform {
  terraform init
  terraform destroy -var-file $SCRIPT_DIR/dev.tfvars -auto-approve
  #terraform plan -var-file $SCRIPT_DIR/dev.tfvars
  terraform apply -var-file $SCRIPT_DIR/dev.tfvars -auto-approve
  rm $SCRIPT_DIR/dev.tfvars
}

SCRIPT_DIR=$(pwd)
cd ../..
JAVA_HOME_OLD=${JAVA_HOME}
export JAVA_HOME=${JAVA_11_HOME}
mvn clean package -Pbuild-and-deploy-flex-template -Dgcp.project.id=${PROJECT}
cd $SCRIPT_DIR

USER=bartek
JOB=${USER}-mypubsubtoconsolejob
BUCKET=${JOB}
TOPIC=${USER}-topic
SUBSCRIPTION=${TOPIC}-sub

#gsutil rm -r gs://${USER}-terraform/MyPubsubToConsoleJob
#gcloud pubsub subscriptions delete ${USER}-topic-sub
#gcloud pubsub topics delete ${USER}-topic
#gsutil rm -r gs://${USER}-mypubsubtoconsolejob
#gcloud dataflow jobs list --filter "NAME:${JOB}-flex-template AND STATE=Running" --format 'value(JOB_ID)' --region "$REGION" | xargs gcloud dataflow jobs cancel --region "$REGION"


export TF_VAR_project="$PROJECT"
export TF_VAR_region="$REGION"
export TF_VAR_label="$USER"
export TF_VAR_bucket="$BUCKET"

>$SCRIPT_DIR/dev.tfvars cat <<-EOF
topic="${TOPIC}"
subscription="${SUBSCRIPTION}"
zone="$ZONE"
job="${JOB}"
service_account="$SERVICE_ACCOUNT"
subnetwork="$SUBNETWORK"
output="gs://${BUCKET}/output"
EOF

run_terraform

export JAVA_HOME=${JAVA_HOME_OLD}

#java -jar ~/Downloads/avro-tools-1.8.1.jar tojson

