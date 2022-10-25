# cleanup
#docker images -a | grep -v bullseye-slim  | grep -v IMAGE | awk '{ print $3 }' | xargs docker image rm -f
#docker ps -aq | xargs docker rm
#docker ps -aq | xargs docker rm -f && docker images -aq | xargs docker image rm -f && docker ps -a && docker images -a

#docker build --build-arg MYARG=ABC -t debian-11-testing .
#docker run --env MYENV=XYZ --env MY_DOCKER_RUN_COMMAND_LINE_ENV=AABBCC debian-11-testing

#docker tag debian-11-docker-testing gcr.io/$GCP_PROJECT/$GCP_OWNER/debian-11-docker-testing
#docker push gcr.io/$GCP_PROJECT/$GCP_OWNER/debian-11-docker-testing


# on cos machine
#docker run -it --rm alpine/gcloud gcloud auth configure-docker
#docker-credential-gcr configure-docker

terraform apply -auto-approve \
 -var="project=$GCP_PROJECT" \
 -var="zone=$GCP_ZONE" \
 -var="subnetwork=projects/$GCP_PROJECT/regions/$GCP_REGION/subnetworks/..." \
 -var="service_account=$GCP_SERVICE_ACCOUNT" \
 -var="image=projects/cos-cloud/global/images/cos-stable-101-17162-40-13"


