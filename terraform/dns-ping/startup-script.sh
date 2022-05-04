#!/bin/bash

LOG="/tmp/startup-script.txt"

echo "Hello world" | tee -a ${LOG}
java -version 2>&1 | tee -a ${LOG}
