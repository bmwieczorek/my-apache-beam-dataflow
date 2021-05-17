resource "time_sleep" "wait_5_seconds" {
  destroy_duration = "5s"
}

resource "google_pubsub_topic" "my_topic" {
  depends_on = [time_sleep.wait_5_seconds]
  project = var.project
  name = var.topic

  labels = {
    user = var.label
  }
}
resource "google_pubsub_subscription" "my_subscription" {
  project = var.project
  name    = var.subscription
  topic   = google_pubsub_topic.my_topic.name

  labels = {
    user = var.label
  }
}
