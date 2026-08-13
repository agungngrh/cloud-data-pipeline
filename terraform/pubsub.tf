resource "google_pubsub_topic" "trip_events" {
  name    = "${var.resource_prefix}-trip-events"
  project = var.project_id
}

resource "google_pubsub_subscription" "trip_events" {
  name    = "${var.resource_prefix}-trip-events-sub"
  project = var.project_id
  topic   = google_pubsub_topic.trip_events.id
}