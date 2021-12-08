locals {
  jobName = "${var.owner}-mybqreadwritejob"
  bucket  = "${var.project}-${local.jobName}"
  body    = <<-EOF
    {
      "jobName": "${local.jobName}",
      "parameters": {
        "expirationDate" : "${var.expiration_date}"
      },
      "environment": {
        "tempLocation": "gs://${var.project}/temp",
        "zone": "${var.zone}",
        "serviceAccountEmail": "${var.service_account}",
        "subnetwork": "${var.subnetwork}"
      }
    }
  EOF
}

resource "google_cloud_scheduler_job" "job" {
  name             = local.jobName
  schedule         = "*/10 * * * *"
  time_zone        = "Europe/Warsaw"
  attempt_deadline = "320s"
  project          = var.project
  region           = var.region

  http_target {
    http_method = "POST"
    uri         = "https://dataflow.googleapis.com/v1b3/projects/${var.project}/locations/${var.region}/templates:launch?gcsPath=gs://${local.bucket}/templates/${local.jobName}-template"
    body        = base64encode("{\"jobName\": \"${local.jobName}-${var.expiration_date}-cron\", \"parameters\": {\"expirationDate\" : \"${var.expiration_date}\"}, \"environment\": {\"tempLocation\": \"gs://${var.project}/temp\", \"zone\": \"${var.zone}\", \"serviceAccountEmail\": \"${var.service_account}\", \"subnetwork\": \"${var.subnetwork}\"}}")
//    body      = filebase64("body.json")
//    body        = base64encode(templatefile("body-template.json", {
//      project             = var.project
//      zone                = var.zone
//      serviceAccountEmail = var.service_account
//      subnetwork          = var.subnetwork
//      jobName             = local.jobName
//      expirationDate      = var.expiration_date
//     }))

    oauth_token {
      service_account_email = var.service_account
      scope                 = "https://www.googleapis.com/auth/cloud-platform"
    }
  }
}
