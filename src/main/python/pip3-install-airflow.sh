#!/bin/bash
AIRFLOW_VERSION=1.10.15
PYTHON_MAJOR_VERSION="$(python3 --version | cut -d " " -f 2 | cut -d "." -f 1-2)"

echo "PYTHON MAJOR VERSION=$PYTHON_MAJOR_VERSION"
# https://raw.githubusercontent.com/apache/airflow/constraints-1.10.15/constraints-3.8.txt
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_MAJOR_VERSION}.txt"
echo "CONSTRAINT URL=$CONSTRAINT_URL"

echo -ne '##########                (50%)\r'
sleep 1
echo -ne '####################      (100%)\r'
echo -ne '\n'

pip3 uninstall -y -r <(pip3 freeze | grep -v my-package)

pip3 --use-deprecated legacy-resolver install \
 "apache-airflow==${AIRFLOW_VERSION}" \
 apache-airflow-backport-providers-apache-beam==2021.3.13 apache-airflow-backport-providers-google==2021.3.3 \
 pytest \
 apache-beam[interactive] \
 pendulum \
 jupyter \
 --constraint "${CONSTRAINT_URL}"
# apache-beam[gcp] \

#pip3 install jupyter
