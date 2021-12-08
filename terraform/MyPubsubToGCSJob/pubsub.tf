resource "time_sleep" "wait_5_seconds" {
  destroy_duration = "5s"
}

resource "google_pubsub_topic" "my_topic" {
  depends_on = [time_sleep.wait_5_seconds]
  project    = var.project
  name       = local.topic
  labels     = local.labels
}
resource "google_pubsub_subscription" "my_subscription" {
  project = var.project
  name    = local.subscription
  topic   = google_pubsub_topic.my_topic.name
  labels  = local.labels
}
