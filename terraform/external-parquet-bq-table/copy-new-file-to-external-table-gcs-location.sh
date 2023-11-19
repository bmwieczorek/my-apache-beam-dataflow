#!/bin/bash
project=$1
echo "Executing: gsutil cp myNestedRecordMoreFields.parquet gs://$project-bartek-external-table/external_table/year=2023/month=01/day=01/hour=01/myNestedRecordMoreFields.parquet"
gsutil cp myNestedRecordMoreFields.parquet \
 gs://$project-bartek-external-table/external_table/year=2023/month=01/day=01/hour=01/myNestedRecordMoreFields.parquet
