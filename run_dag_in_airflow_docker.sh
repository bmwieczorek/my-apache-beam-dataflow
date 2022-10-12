#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "[ERROR] missing dag_name argument"
    echo "Usage: $0 dag_name"
    echo "e.g.: $0 bartek_dag"
    exit 1;
fi

dag=$1

if [[ $dag == *.py ]]; then
    echo "[ERROR] remove .py suffix from dag_name argument"
    echo "Usage: $0 dag_name"
    echo "e.g.: $0 bartek_dag"
    exit 1;
fi

eval "$(docker-machine env default)"

echo "Deploying $dag to airflow running in local docker ..."

dir_shared_with_docker=~/dev/my-docker/share

echo "Current local date time: $(date '+%Y-%m-%dT%H:%M:%S%z')"
echo "Current UTC   date time: $(date -u '+%Y-%m-%dT%H:%M:%S%z')"

sed "s/_SCHEDULE_INTERVAL_/$(date -u -v +3M '+%M %H %d') * */" "src/main/python/airflow/${dag}.py" | \
sed "s/_START_DATE_/$(date -u -v -1m -v +3M +%Y-%m-%dT%H:%M:00)/" > "${dir_shared_with_docker}/${dag}.py" && \
docker exec -t airflow bash -c "cp /share/${dag}.py ~/airflow/dags/ && airflow dags list" && \
until docker exec -t airflow bash -c "airflow dags unpause ${dag}"; do echo "Waiting 3 secs to unpause ${dag} dag ..." && sleep 3; done
