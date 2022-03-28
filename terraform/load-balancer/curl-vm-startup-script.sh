#!/bin/bash

LOG="/tmp/startup-script.txt"

LB_IP=$(curl http://metadata.google.internal/computeMetadata/v1/instance/attributes/lb_ip -H "Metadata-Flavor: Google")
echo "LB_IP=$LB_IP" | tee -a ${LOG}

LB_PORT=$(curl http://metadata.google.internal/computeMetadata/v1/instance/attributes/lb_port -H "Metadata-Flavor: Google")
echo "LB_PORT=$LB_PORT" | tee -a ${LOG}

LB_DNS_FULL_NAME=$(curl http://metadata.google.internal/computeMetadata/v1/instance/attributes/lb_dns_full_name -H "Metadata-Flavor: Google")
echo "LB_DNS_FULL_NAME=$LB_DNS_FULL_NAME" | tee -a ${LOG}

echo "------------------------------------------------------------------------------------" | tee -a ${LOG}
echo "--- Hello world curl. Curl $LB_IP and $LB_DNS_FULL_NAME ---" | tee -a ${LOG}
echo "------------------------------------------------------------------------------------" | tee -a ${LOG}
host $LB_DNS_FULL_NAME

timeout 300 bash -c 'while [[ "$(curl -s -o /dev/null -w ''%{http_code}'' http://$LB_DNS_FULL_NAME:$LB_PORT)" != "200" ]]; do sleep 5; done'

curl http://$LB_IP:$LB_PORT
curl http://$LB_DNS_FULL_NAME:$LB_PORT
curl http://$LB_DNS_FULL_NAME:$LB_PORT
curl http://$LB_DNS_FULL_NAME:$LB_PORT
curl http://$LB_DNS_FULL_NAME:$LB_PORT
curl http://$LB_DNS_FULL_NAME:$LB_PORT
curl http://$LB_DNS_FULL_NAME:$LB_PORT
curl http://$LB_DNS_FULL_NAME:$LB_PORT
