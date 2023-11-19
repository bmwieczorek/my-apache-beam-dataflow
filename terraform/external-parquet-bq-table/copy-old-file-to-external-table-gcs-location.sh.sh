#!/bin/bash
project=$1
echo "Executing: gsutil cp myNestedRecordFewerFields.parquet gs://$project-bartek-external-table/external_table/year=2023/month=01/day=01/hour=00/myNestedRecordFewerFields.parquet"
gsutil cp myNestedRecordFewerFields.parquet \
 gs://$project-bartek-external-table/external_table/year=2023/month=01/day=01/hour=00/myNestedRecordFewerFields.parquet
