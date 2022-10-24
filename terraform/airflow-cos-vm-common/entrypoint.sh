#!/bin/bash
echo "PATH=$PATH"
echo "BUCKET=$BUCKET"
cat $HOME/.bash_profile

source $HOME/.bash_profile
pyenv versions
python3 -V

mkdir ~/mnt
gcsfuse $BUCKET ~/mnt
ln -s ~/mnt/dags ~/airflow/dags
ls -la ~/airflow/dags/*

nohup airflow webserver $* >> ~/airflow/logs/webserver.logs &
nohup airflow scheduler >> ~/airflow/logs/scheduler.logs &
sleep 5
tail -f ~/airflow/logs/webserver.logs  ~/airflow/logs/scheduler.logs

