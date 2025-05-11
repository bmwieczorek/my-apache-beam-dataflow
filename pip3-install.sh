#!/bin/bash
pip3 uninstall -y -r <(pip3 freeze | grep -v my-package)

pip3 install --upgrade pip

#pip3 install \
# google-cloud-bigquery \
# --constraint constraints.txt

pip3 install \
 -r requirements.txt \
 --constraint constraints.txt
