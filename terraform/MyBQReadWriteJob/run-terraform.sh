#!/bin/bash

function run_terraform_all {
  service=$1
  echo "service=$service"
  cd "$SCRIPT_DIR/$service" || exit
  terraform init
  terraform destroy -var-file "$SCRIPT_DIR/$service/$service.tfvars" -auto-approve
  terraform apply -var-file "$SCRIPT_DIR/$service/$service.tfvars" -auto-approve
  rm "$SCRIPT_DIR/$service/$service.tfvars"
}

function run_terraform_apply {
  service=$1
  echo "service=$service"
  cd "$SCRIPT_DIR/$service" || exit
  terraform apply -var-file "$SCRIPT_DIR/$service/$service.tfvars" -auto-approve
  rm "$SCRIPT_DIR/$service/$service.tfvars"
}

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd ../..
JAVA_HOME_OLD=${JAVA_HOME}
export JAVA_HOME=${JAVA_11_HOME}
mvn clean package -Pdist -DskipTests
cd "$SCRIPT_DIR" || exit

if [ -z "$OWNER" ]
then
  OWNER=$USER
fi
echo "OWNER=$OWNER"

JOB=$(echo "${OWNER}-$(basename $SCRIPT_DIR)" | tr '[:upper:]' '[:lower:]' )
echo "JOB=$JOB"
BUCKET=${JOB}
echo "BUCKET=$BUCKET"
EXPIRATION_DATE=2021-03-03

echo "Deleting manually resources owned by $OWNER ..."
gsutil -m rm -r "gs://${OWNER}-terraform/$(basename $SCRIPT_DIR)"
bq rm -r -f -d ${PROJECT}:${OWNER}_dataset
gcloud beta logging sinks delete "${JOB}-logging-sink" --quiet
gcloud monitoring dashboards list --filter="displayName='$JOB redesigned job id'" --format 'value(NAME)' | xargs gcloud monitoring dashboards delete --quiet
gcloud monitoring dashboards list --filter="displayName='$JOB redesigned job name'" --format 'value(NAME)' | xargs gcloud monitoring dashboards delete --quiet
for name in $(gcloud alpha monitoring policies list --filter "display_name='bartek-mybqreadwritejob last run did not run for last 300s alert policy'" --format 'value(NAME)'); do gcloud alpha monitoring policies delete $name --quiet ; done
max_retry=20; counter=1; sleep_secs=5; until [ -z "$(gcloud dataflow jobs list --filter "NAME:${JOB}-${EXPIRATION_DATE} AND (STATE=Cancelling OR STATE=Running)" --format 'value(JOB_ID)' --region $REGION)" ] ; do sleep $sleep_secs; [[ counter -eq $max_retry ]] && echo "Failed" && break; echo "waiting $sleep_secs secs for job to stop: attempt $counter" ; ((counter++)); done
gsutil rm -r "gs://${JOB}"
gsutil -q stat "gs://$OWNER-terraform/**" || gsutil mb "gs://$OWNER-terraform"

export TF_VAR_project="$PROJECT"
export TF_VAR_region="$REGION"
export TF_VAR_owner="$OWNER"
export TF_VAR_bucket="$BUCKET"


>"$SCRIPT_DIR/storage/storage.tfvars" cat <<-EOF
EOF

#>$SCRIPT_DIR/dashboard/dashboard.tfvars cat <<-EOF
#dashboard_file="dashboard.json.old"
#job="${JOB}"
#EOF

#>$SCRIPT_DIR/alerting/alerting.tfvars cat <<-EOF
#job="${JOB}"
#notification_email="${EMAIL}"
#EOF

>"$SCRIPT_DIR/bigquery/bigquery.tfvars" cat <<-EOF
dataset="${OWNER}_dataset"
table="mysubscription_table"
view="mysubscription_view"
table_schema_file="../../../target/MyBQReadWriteJob.json"
load_file="mysubscription_table.json"
EOF

>"$SCRIPT_DIR/dataflow/dataflow.tfvars" cat <<-EOF
service_account="$SERVICE_ACCOUNT"
subnetwork="$SUBNETWORK"
job="${JOB}"
expiration_date="${EXPIRATION_DATE}"
zone="$ZONE"
instance="${OWNER}-vm"
image="$IMAGE"
dataflow_jar="my-apache-beam-dataflow-0.1-SNAPSHOT.jar"
dataflow_start_time="$(date -u "+%Y-%m-%dT%H:%M:%SZ")"
dashboard_file="dashboard.json"
notification_email="${EMAIL}"
EOF

run_terraform_all storage
run_terraform_all bigquery
#run_terraform dashboard
#run_terraform alerting


#cd $SCRIPT_DIR/../..
### Create template from java ###
#mvn clean package -DskipTests -Pmake-dist -Pdataflow-runner
#java -cp target/my-apache-beam-dataflow-0.1-SNAPSHOT.jar com.bawi.beam.dataflow.MyBQReadWriteJob \
# ${JAVA_DATAFLOW_RUN_OPTS} \
# --runner=DataflowRunner \
# --stagingLocation=gs://${BUCKET}/staging \
# --templateLocation=gs://${BUCKET}/templates/${JOB}-template

### Execute from template ###
#gcloud dataflow jobs run ${JOB}-${OWNER}-template-${EXPIRATION_DATE} \
#  ${GCLOUD_DATAFLOW_RUN_OPTS} \
#  --gcs-location gs://${BUCKET}/templates/${JOB}-template \
#  --parameters expirationDate=${EXPIRATION_DATE}

run_terraform_all dataflow

export JAVA_HOME=${JAVA_HOME_OLD}
