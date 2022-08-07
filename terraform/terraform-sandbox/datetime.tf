output "timestamp_formatted" {
  value = formatdate("YYYY-MM-DD'T'hh:mm:ss'.000Z'", timestamp())
}

output "timestamp_formatted-min-10-mins" {
  value = "${formatdate("YYYY-MM-DD'T'hh:mm:ss", timeadd(timestamp(),"-10m"))}+00:00"
}
