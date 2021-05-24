#!/bin/bash

function run_terraform {
  terraform init
  terraform destroy -var-file $SCRIPT_DIR/dev.tfvars -auto-approve
  terraform apply -var-file $SCRIPT_DIR/dev.tfvars -auto-approve
  rm $SCRIPT_DIR/dev.tfvars
}

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd ../..
JAVA_HOME_OLD=${JAVA_HOME}
export JAVA_HOME=${JAVA_11_HOME}
mvn clean package -Pbuild-and-deploy-flex-template -Dgcp.project.id=${PROJECT}
cd $SCRIPT_DIR

if [ -z "$OWNER" ]
then
  OWNER=$USER
fi
echo "OWNER=$OWNER"

JOB=$(echo "${OWNER}-$(basename $SCRIPT_DIR)" | tr '[:upper:]' '[:lower:]' )
echo "JOB=$JOB"
BUCKET=${JOB}
echo "BUCKET=$BUCKET"
TOPIC=${OWNER}-topic
echo "TOPIC=$TOPIC"
SUBSCRIPTION=${TOPIC}-sub
echo "SUBSCRIPTION=$SUBSCRIPTION"

echo "Deleting manually resources owned by $OWNER ..."
gsutil rm -r gs://${OWNER}-terraform/${JOB}
gcloud pubsub subscriptions delete ${SUBSCRIPTION}
gcloud pubsub topics delete ${TOPIC}
gcloud monitoring dashboards list --filter="displayName='${JOB} job id'" --format 'value(NAME)' | xargs gcloud monitoring dashboards delete --quiet
gcloud monitoring dashboards list --filter="displayName='${JOB} job name'" --format 'value(NAME)' | xargs gcloud monitoring dashboards delete --quiet
gcloud dataflow jobs list --filter "NAME:${JOB} AND STATE=Running" --format 'value(JOB_ID)' --region "$REGION" | xargs gcloud dataflow jobs cancel --region "$REGION"
max_retry=10; counter=1; sleep_secs=5; until [ -z "$(gcloud dataflow jobs list --filter "NAME:${JOB} AND (STATE=Cancelling OR STATE=Running)" --format 'value(JOB_ID)' --region $REGION)" ] ; do sleep $sleep_secs; [[ counter -eq $max_retry ]] && echo "Failed" && break; echo "waiting $sleep_secs secs for job to stop: attempt $counter" ; ((counter++)); done
gsutil rm -r gs://${JOB}

export TF_VAR_project="$PROJECT"
export TF_VAR_region="$REGION"
export TF_VAR_owner="$OWNER"
export TF_VAR_bucket="$BUCKET"

gsutil -q stat "gs://$OWNER-terraform/**" || gsutil mb "gs://$OWNER-terraform"

>$SCRIPT_DIR/backend.tf cat <<-EOF
terraform {
  backend "gcs" {
    bucket = "$OWNER-terraform"
    prefix = "$JOB/tfstate"
  }
  required_providers {
    google = {
      version = "~> 3.58.0"
    }
  }
  required_version = "~> 0.12.29"
}
EOF


>$SCRIPT_DIR/dev.tfvars cat <<-EOF
topic="${TOPIC}"
subscription="${SUBSCRIPTION}"
zone="$ZONE"
job="${JOB}"
service_account="$SERVICE_ACCOUNT"
subnetwork="$SUBNETWORK"
output="gs://${BUCKET}/output"
dashboard_file="dashboard.json"
EOF

run_terraform

export JAVA_HOME=${JAVA_HOME_OLD}

#java -jar ~/Downloads/avro-tools-1.8.1.jar tojson

