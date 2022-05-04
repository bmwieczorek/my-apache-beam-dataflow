#!/bin/bash

LOG="/tmp/startup-script.txt"

VM_IP_TO_PING=$(curl http://metadata.google.internal/computeMetadata/v1/instance/attributes/vm_ip_to_ping -H "Metadata-Flavor: Google")
echo "VM_IP_TO_PING=$VM_IP_TO_PING" | tee -a ${LOG}
DNS_VM_FULL_NAME_TO_PING=$(curl http://metadata.google.internal/computeMetadata/v1/instance/attributes/dns_vm_full_name_to_ping -H "Metadata-Flavor: Google")
echo "DNS_VM_FULL_NAME_TO_PING=$DNS_VM_FULL_NAME_TO_PING" | tee -a ${LOG}

echo "Hello world ping. Pinging $VM_IP_TO_PING and $DNS_VM_FULL_NAME_TO_PING" | tee -a ${LOG}
ping -c 3 $VM_IP_TO_PING
ping -c 3 $DNS_VM_FULL_NAME_TO_PING
host $DNS_VM_FULL_NAME_TO_PING

