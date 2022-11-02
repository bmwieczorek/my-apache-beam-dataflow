#!/bin/bash
AIRFLOW_VERSION=1.10.15
PYTHON_MAJOR_VERSION="$(python3 --version | cut -d " " -f 2 | cut -d "." -f 1-2)"

echo "PYTHON MAJOR VERSION=$PYTHON_MAJOR_VERSION"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_MAJOR_VERSION}.txt"
echo "CONSTRAINT URL=$CONSTRAINT_URL"

echo -ne '##########                (50%)\r'
sleep 1
echo -ne '####################      (100%)\r'
echo -ne '\n'

pip3 --use-deprecated legacy-resolver install \
 "apache-airflow==${AIRFLOW_VERSION}" \
 apache-airflow-backport-providers-apache-beam==2021.3.13 apache-airflow-backport-providers-google==2021.3.3 \
 pytest \
 --constraint "${CONSTRAINT_URL}"
