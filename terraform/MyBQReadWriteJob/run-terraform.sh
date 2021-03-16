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

USER=bartek
JOB_NAME=mybqreadwritejob
BUCKET=${PROJECT}-${USER}-${JOB_NAME}
EXPIRATION_DATE=2021-03-03
SCRIPT_DIR=$(PWD)

export TF_VAR_project="$PROJECT"
export TF_VAR_region="$REGION"
export TF_VAR_label="$USER"
export TF_VAR_bucket="$BUCKET"

>$SCRIPT_DIR/storage/storage.tfvars cat <<-EOF
EOF

>$SCRIPT_DIR/bigquery/bigquery.tfvars cat <<-EOF
dataset="${USER}_dataset"
table="mysubscription_table"
load_file="mysubscription_table.csv"
EOF

>$SCRIPT_DIR/dataflow/dataflow.tfvars cat <<-EOF
service_account="$SERVICE_ACCOUNT"
subnetwork="$SUBNETWORK"
job="${JOB_NAME}"
expiration_date="${EXPIRATION_DATE}"
EOF

run_terraform storage
run_terraform bigquery

cd $SCRIPT_DIR/../..
### Create template from java ###
mvn clean package -DskipTests -Pmake-dist -Pdataflow-runner
java -cp target/my-apache-beam-dataflow-0.1-SNAPSHOT.jar com.bawi.beam.dataflow.MyBQReadWriteJob \
 ${JAVA_DATAFLOW_RUN_OPTS} \
 --runner=DataflowRunner \
 --stagingLocation=gs://${BUCKET}/staging \
 --templateLocation=gs://${BUCKET}/templates/${JOB_NAME}-template

### Execute from template ###
#gcloud dataflow jobs run ${JOB_NAME}-${USER}-template-${EXPIRATION_DATE} \
#  ${GCLOUD_DATAFLOW_RUN_OPTS} \
#  --gcs-location gs://${BUCKET}/templates/${JOB_NAME}-template \
#  --parameters expirationDate=${EXPIRATION_DATE}

run_terraform dataflow


