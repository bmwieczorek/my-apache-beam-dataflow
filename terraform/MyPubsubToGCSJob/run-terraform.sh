#!/bin/bash

function run_terraform {
  terraform init
  terraform destroy -var-file $SCRIPT_DIR/dev.tfvars -auto-approve
  terraform apply -var-file $SCRIPT_DIR/dev.tfvars -auto-approve
  rm $SCRIPT_DIR/dev.tfvars
}

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd ../..
mvn clean package -Pbuild-and-deploy-flex-template -Dgcp.project.id=${GCP_PROJECT} -DskipTests
cd $SCRIPT_DIR

if [ -z "${GCP_OWNER}" ]
then
  GCP_OWNER=$USER
else
  GCP_OWNER=${GCP_OWNER}
fi
echo "GCP_OWNER=$GCP_OWNER"

JOB=$(echo "${GCP_OWNER}-$(basename $SCRIPT_DIR)" | tr '[:upper:]' '[:lower:]' )
echo "JOB=$JOB"
BUCKET="${GCP_PROJECT}-${JOB}"
echo "BUCKET=$BUCKET"
TOPIC=${GCP_OWNER}-topic
echo "TOPIC=$TOPIC"
SUBSCRIPTION=${TOPIC}-sub
echo "SUBSCRIPTION=$SUBSCRIPTION"

echo "Deleting manually resources owned by $GCP_OWNER ..."
gsutil rm -r gs://${GCP_OWNER}-terraform/${JOB}
gcloud pubsub subscriptions delete ${SUBSCRIPTION}
gcloud pubsub topics delete ${TOPIC}
gcloud monitoring dashboards list --filter="displayName='${JOB} job id'" --format 'value(NAME)' | xargs gcloud monitoring dashboards delete --quiet
gcloud monitoring dashboards list --filter="displayName='${JOB} job name'" --format 'value(NAME)' | xargs gcloud monitoring dashboards delete --quiet
gcloud monitoring dashboards list --filter="displayName='${JOB} redesigned job id'" --format 'value(NAME)' | xargs gcloud monitoring dashboards delete --quiet
gcloud monitoring dashboards list --filter="displayName='${JOB} redesigned job name'" --format 'value(NAME)' | xargs gcloud monitoring dashboards delete --quiet

gcloud dataflow jobs list --filter "NAME:${JOB} AND STATE=Running" --format 'value(JOB_ID)' --region "$GCP_REGION" | xargs gcloud dataflow jobs cancel --region "${GCP_REGION}"
max_retry=10; counter=1; sleep_secs=5; until [ -z "$(gcloud dataflow jobs list --filter "NAME:${JOB} AND (STATE=Cancelling OR STATE=Running)" --format 'value(JOB_ID)' --region ${GCP_REGION})" ] ; do sleep $sleep_secs; [[ counter -eq $max_retry ]] && echo "Failed" && break; echo "waiting $sleep_secs secs for job to stop: attempt $counter" ; ((counter++)); done

gsutil rm -r gs://${BUCKET}
gsutil -q stat "gs://${GCP_OWNER}-terraform/**" || gsutil mb "gs://${GCP_OWNER}-terraform"

export TF_VAR_project="$GCP_PROJECT"
export TF_VAR_region="$GCP_REGION"
export TF_VAR_owner="$GCP_OWNER"
export TF_VAR_bucket="$BUCKET"


>$SCRIPT_DIR/backend.tf cat <<-EOF
terraform {
  backend "gcs" {
    bucket = "$GCP_PROJECT-$GCP_OWNER-terraform"
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
zone="$GCP_ZONE"
job="${JOB}"
service_account="$GCP_SERVICE_ACCOUNT"
subnetwork="$GCP_SUBNETWORK"
output="gs://${BUCKET}/output"
dashboard_file="dashboard.json"
EOF

run_terraform


