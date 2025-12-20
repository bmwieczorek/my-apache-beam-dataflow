[System.Environment]::SetEnvironmentVariable('GCP_REGION','us-central1', 'User')
[System.Environment]::SetEnvironmentVariable('GCP_EMAIL','bartosz.wieczorek@sabre.com', 'User')
[System.Environment]::SetEnvironmentVariable('GCP_REGION','us-central1', 'User')
[System.Environment]::SetEnvironmentVariable('GCP_ZONE','us-central1-f', 'User')
[System.Environment]::SetEnvironmentVariable('GCP_OWNER','bartek', 'User')
[System.Environment]::SetEnvironmentVariable('GCP_IMAGE','sab-dev-dap-data-pipeline-3013/dnacommon-j17-2-0-4-2410012030', 'User')
[System.Environment]::SetEnvironmentVariable('GCP_BUCKET','sab-dev-dap-data-pipeline-3013-bartek', 'User')
[System.Environment]::SetEnvironmentVariable('GCP_SUBNETWORK','https://www.googleapis.com/compute/v1/projects/sab-ssvcs-network-vpcs-5041/regions/us-central1/subnetworks/sn-dev-uscentral1-03', 'User')
[System.Environment]::SetEnvironmentVariable('GCP_PROJECT','sab-dev-dap-data-pipeline-3013', 'User')
[System.Environment]::SetEnvironmentVariable('GCP_SERVICE_ACCOUNT','sab-dev-dap-airshopping@sab-dev-dap-data-pipeline-3013.iam.gserviceaccount.com', 'User')
[System.Environment]::SetEnvironmentVariable('GCP_JAVA_DATAFLOW_RUN_OPTS','--project=sab-dev-dap-data-pipeline-3013 --region=us-central1  --serviceAccount=sab-dev-dap-airshopping@sab-dev-dap-data-pipeline-3013.iam.gserviceaccount.com  --subnetwork=https://www.googleapis.com/compute/v1/projects/sab-ssvcs-network-vpcs-5041/regions/us-central1/subnetworks/sn-dev-uscentral1-03  --usePublicIps=false', 'User')

[System.Environment]::SetEnvironmentVariable('TF_VAR_service_account','sab-dev-dap-airshopping@sab-dev-dap-data-pipeline-3013.iam.gserviceaccount.com', 'User')
[System.Environment]::SetEnvironmentVariable('TF_VAR_notification_email','bartosz.wieczorek@sabre.com', 'User')
[System.Environment]::SetEnvironmentVariable('TF_VAR_image','sab-dev-dap-data-pipeline-3013/dnacommon-j17-2-0-4-2410012030', 'User')
[System.Environment]::SetEnvironmentVariable('TF_VAR_owner','bartek', 'User')
[System.Environment]::SetEnvironmentVariable('TF_VAR_project','sab-dev-dap-data-pipeline-3013', 'User')
[System.Environment]::SetEnvironmentVariable('TF_VAR_subnetwork','https://www.googleapis.com/compute/v1/projects/sab-ssvcs-network-vpcs-5041/regions/us-central1/subnetworks/sn-dev-uscentral1-03', 'User')
[System.Environment]::SetEnvironmentVariable('TF_VAR_region','us-central1', 'User')
[System.Environment]::SetEnvironmentVariable('TF_VAR_zone','us-central1-f', 'User')

$userpath = [System.Environment]::GetEnvironmentVariable("PATH","USER")
$userpath = $userpath + ";%USERPROFILE%\AppData\Local\Microsoft\WindowsApps"
$userpath = $userpath + ";%USERPROFILE%\AppData\Local\Google\Cloud SDK\google-cloud-sdk\bin"
$userpath = $userpath + ";%USERPROFILE%\dev\env\apache-maven-3.9.12\bin"
$userpath = $userpath + ";%USERPROFILE%\dev\env\terraform_0.12.31_windows_amd64"
[System.Environment]::SetEnvironmentVariable("PATH",$userpath,"USER")

git config --global core.autocrlf input
# If you already have checked out the code, the files are already indexed. After changing your Git settings, say by running command below. Note: this will remove your local changes. Consider stashing them before you do this.
# git rm --cached -r .
# git reset --hard
