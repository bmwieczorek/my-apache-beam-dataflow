resource "google_monitoring_notification_channel" "email" {
  project      = var.project
  enabled      = true
  display_name = "${var.job} alert - ${var.notification_email}"
  type         = "email"

  labels = {
    email_address = var.notification_email
  }

  user_labels = {
    owner = var.owner
  }
}