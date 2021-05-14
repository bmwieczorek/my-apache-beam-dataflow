#!/bin/bash

function run_terraform {
  service=$1
  cd $SCRIPT_DIR/$service
  terraform init
  terraform destroy -var-file $SCRIPT_DIR/$service/$service.tfvars -auto-approve
  terraform plan -var-file $SCRIPT_DIR/$service/$service.tfvars
  terraform apply -var-file $SCRIPT_DIR/$service/$service.tfvars -auto-approve
  rm $SCRIPT_DIR/$service/$service.tfvars
}

SCRIPT_DIR=$(pwd)
cd ../..
#JAVA_HOME_OLD=${JAVA_HOME}
#export JAVA_HOME=${JAVA_11_HOME}
mvn clean package -Pdist
cd $SCRIPT_DIR

USER=bartek
JOB=${USER}-mybqreadwritejob
BUCKET=${JOB}
EXPIRATION_DATE=2021-03-03

# gsutil rm -r gs://${JOB}
# gsutil rm -r gs://${USER}-terraform/*
# bq rm -r -f -d ${PROJECT}:${USER}_dataset
# gcloud beta logging sinks delete "${JOB}-logging-sink" --quiet

export TF_VAR_project="$PROJECT"
export TF_VAR_region="$REGION"
export TF_VAR_label="$USER"
export TF_VAR_bucket="$BUCKET"

>$SCRIPT_DIR/storage/storage.tfvars cat <<-EOF
EOF

>$SCRIPT_DIR/dashboard/dashboard.tfvars cat <<-EOF
dashboard_file="dashboard.json"
job="${JOB}"
EOF

>$SCRIPT_DIR/alerting/alerting.tfvars cat <<-EOF
job="${JOB}"
notification_email="${EMAIL}"
EOF

>$SCRIPT_DIR/bigquery/bigquery.tfvars cat <<-EOF
dataset="${USER}_dataset"
table="mysubscription_table"
view="mysubscription_view"
table_schema_file="../../../target/MyBQReadWriteJob.json"
load_file="mysubscription_table.json"
EOF

>$SCRIPT_DIR/dataflow/dataflow.tfvars cat <<-EOF
service_account="$SERVICE_ACCOUNT"
subnetwork="$SUBNETWORK"
job="${JOB}"
expiration_date="${EXPIRATION_DATE}"
zone="$ZONE"
instance="${USER}-vm"
image="$IMAGE"
dataflow_jar="my-apache-beam-dataflow-0.1-SNAPSHOT.jar"
dataflow_start_time="$(date -u "+%Y-%m-%dT%H:%M:%SZ")"
dashboard_file="dashboard-last.json"
notification_email="${EMAIL}"
EOF

run_terraform storage
run_terraform bigquery
run_terraform dashboard
run_terraform alerting


#cd $SCRIPT_DIR/../..
### Create template from java ###
#mvn clean package -DskipTests -Pmake-dist -Pdataflow-runner
#java -cp target/my-apache-beam-dataflow-0.1-SNAPSHOT.jar com.bawi.beam.dataflow.MyBQReadWriteJob \
# ${JAVA_DATAFLOW_RUN_OPTS} \
# --runner=DataflowRunner \
# --stagingLocation=gs://${BUCKET}/staging \
# --templateLocation=gs://${BUCKET}/templates/${JOB}-template

### Execute from template ###
#gcloud dataflow jobs run ${JOB}-${USER}-template-${EXPIRATION_DATE} \
#  ${GCLOUD_DATAFLOW_RUN_OPTS} \
#  --gcs-location gs://${BUCKET}/templates/${JOB}-template \
#  --parameters expirationDate=${EXPIRATION_DATE}

run_terraform dataflow

#export JAVA_HOME=${JAVA_HOME_OLD}
