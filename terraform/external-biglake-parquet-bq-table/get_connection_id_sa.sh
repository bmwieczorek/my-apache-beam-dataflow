#!/bin/bash
BQ_PROJECT=$1
LOCATION=$2
bq ls --connection --location=${LOCATION} --project_id=${BQ_PROJECT} --format=prettyjson | jq ".[] | select(.description != null) | select(.description | contains(\"${BQ_PROJECT}\")) | .cloudResource"
