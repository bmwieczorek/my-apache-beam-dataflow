resource "google_monitoring_notification_channel" "email" {
  project      = var.project
  enabled      = true
  display_name = "${var.job_base_name} alert - ${var.notification_email}"
  type         = "email"

  labels = {
    email_address = var.notification_email
  }

  user_labels = {
    owner = var.owner
  }

//  for_each     = toset(var.notification_emails)
//  display_name = "Notification channel ${each.value}"
//  labels = {
//    email_address = each.value
//  }
}