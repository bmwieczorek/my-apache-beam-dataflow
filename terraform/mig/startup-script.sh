#!/bin/bash
LOG="/tmp/startup-script.log"

echo "Checking java version" | tee -a ${LOG}
java -version 2>&1 | tee -a ${LOG}

echo "Copying spring boot from gsc" | tee -a ${LOG}
gsutil cp gs://bartek-spring-boot/spring-boot-0.0.1-SNAPSHOT.jar . 2>&1 | tee -a ${LOG}

echo "Starting java spring boot" | tee -a ${LOG}
nohup java -jar spring-boot-0.0.1-SNAPSHOT.jar >> ${LOG} 2>&1&
tail -f ${LOG}
