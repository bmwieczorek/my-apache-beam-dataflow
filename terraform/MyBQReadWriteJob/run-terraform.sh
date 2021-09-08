#!/bin/bash

function run_terraform_all {
  service=$1
  echo "service=$service"
  cd "$SCRIPT_DIR/$service" || exit
  terraform init -reconfigure -backend-config="$SCRIPT_DIR/$PROJECT.tfbackend"
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

. ~/.gcp

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
BUCKET=${PROJECT}-${JOB}
echo "BUCKET=$BUCKET"
EXPIRATION_DATE=2021-03-03

echo "Deleting manually resources owned by $OWNER ..."
gsutil -m rm -r "gs://${PROJECT}-${OWNER}-terraform/$(basename $SCRIPT_DIR)"
bq rm -r -f -d ${PROJECT}:${OWNER}_dataset
gcloud beta logging sinks delete "${JOB}-logging-sink" --quiet
gcloud monitoring dashboards list --filter="displayName='$JOB redesigned job id'" --format 'value(NAME)' | xargs gcloud monitoring dashboards delete --quiet
gcloud monitoring dashboards list --filter="displayName='$JOB redesigned job name'" --format 'value(NAME)' | xargs gcloud monitoring dashboards delete --quiet
for name in $(gcloud alpha monitoring policies list --filter "display_name~'bartek-mybqreadwritejob.*'" --format 'value(NAME)'); do gcloud alpha monitoring policies delete $name --quiet ; done
for name in $(gcloud logging metrics list --filter "name~'bartek-mybqreadwritejob.*'" --format 'value(NAME)'); do gcloud logging metrics delete $name --quiet; done
for name in $(gcloud beta monitoring channels list --filter "labels.email_address='${EMAIL}'" --format 'value(NAME)'); do gcloud beta monitoring channels delete $name --quiet ; done
max_retry=20; counter=1; sleep_secs=5; until [ -z "$(gcloud dataflow jobs list --filter "NAME:${JOB}-${EXPIRATION_DATE} AND (STATE=Cancelling OR STATE=Running)" --format 'value(JOB_ID)' --region $REGION)" ] ; do sleep $sleep_secs; [[ counter -eq $max_retry ]] && echo "Failed" && break; echo "waiting $sleep_secs secs for job to stop: attempt $counter" ; ((counter++)); done
gsutil rm -r "gs://${BUCKET}"
gsutil -q stat "gs://${PROJECT}-$OWNER-terraform/**" || gsutil mb "gs://${PROJECT}-$OWNER-terraform"

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

>"$SCRIPT_DIR/dataflow_template/dataflow_template.tfvars" cat <<-EOF
service_account="$SERVICE_ACCOUNT"
network="$NETWORK"
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
main_class="com.bawi.beam.dataflow.MyBQReadWriteJob"
EOF

>"$SCRIPT_DIR/dataflow/dataflow.tfvars" cat <<-EOF
service_account="$SERVICE_ACCOUNT"
network="$NETWORK"
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
main_class="com.bawi.beam.dataflow.MyBQReadWriteJob"
EOF

run_terraform_all storage
#run_terraform_apply storage
run_terraform_all bigquery
#run_terraform_apply bigquery
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
#gcloud dataflow jobs run ${JOB}-template-${EXPIRATION_DATE} \
#  ${GCLOUD_DATAFLOW_RUN_OPTS} \
#  --gcs-location gs://${BUCKET}/templates/${JOB}-template \
#  --parameters expirationDate=${EXPIRATION_DATE}

run_terraform_all dataflow_template
#run_terraform_apply dataflow_template

if [ -f "$GOOGLE_APPLICATION_CREDENTIALS__JSON_FILE" ]
then
  echo "Exporting GOOGLE_APPLICATION_CREDENTIALS=$GOOGLE_APPLICATION_CREDENTIALS__JSON_FILE"
  export GOOGLE_APPLICATION_CREDENTIALS=$GOOGLE_APPLICATION_CREDENTIALS__JSON_FILE
fi

run_terraform_all dataflow
#run_terraform_apply dataflow

export JAVA_HOME=${JAVA_HOME_OLD}

#gcloud dataflow jobs run bartek-mybqreadwritejob-template-2021-03-03 \
#  ${GCLOUD_DATAFLOW_RUN_OPTS} \
#  --gcs-location gs://$PROJECT-bartek-mybqreadwritejob/templates/bartek-mybqreadwritejob-template \
#  --parameters expirationDate=2021-03-03