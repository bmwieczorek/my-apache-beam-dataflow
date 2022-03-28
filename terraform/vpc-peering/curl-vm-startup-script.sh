#!/bin/bash

LOG="/tmp/startup-script.txt"

IP=$(curl http://metadata.google.internal/computeMetadata/v1/instance/attributes/ip -H "Metadata-Flavor: Google")
echo "IP=$IP" | tee -a ${LOG}

PORT=$(curl http://metadata.google.internal/computeMetadata/v1/instance/attributes/port -H "Metadata-Flavor: Google")
echo "PORT=$PORT" | tee -a ${LOG}

#sudo apt-get update --allow-releaseinfo-change
#sudo apt-get install redis-tools

echo "------------------------------------------------------------------------------------" | tee -a ${LOG}
echo "--- Hello world curl. Curl $IP and $PORT ---" | tee -a ${LOG}
echo "------------------------------------------------------------------------------------" | tee -a ${LOG}

timeout 300 bash -c 'while [[ "$(curl -s -o /dev/null -w ''%{http_code}'' http://$IP:$PORT)" != "200" ]]; do sleep 5; done'

curl http://$IP:$PORT
curl http://$IP:$PORT
curl http://$IP:$PORT
curl http://$IP:$PORT
curl http://$IP:$PORT

