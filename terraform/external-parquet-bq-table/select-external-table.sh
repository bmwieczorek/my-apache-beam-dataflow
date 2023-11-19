#!/bin/bash
echo "Executing: bq query --use_legacy_sql=false \"SELECT * FROM $1.bartek_external_table.external_table\""
bq query --use_legacy_sql=false "SELECT * FROM $1.bartek_external_table.external_table"
