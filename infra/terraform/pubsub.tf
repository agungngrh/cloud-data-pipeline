resource "google_pubsub_schema" "trip_event" {
  name    = "${var.resource_prefix}-trip-event-schema"
  project = var.project_id
  type    = "AVRO"

  definition = file("${path.module}/schemas/trip_event.avsc")
}

resource "google_pubsub_topic" "trip_events" {
  name    = "${var.resource_prefix}-trip-events"
  project = var.project_id

  schema_settings {
    schema   = google_pubsub_schema.trip_event.id
    encoding = "JSON"
  }
}

resource "google_pubsub_subscription" "trip_events" {
  name    = "${var.resource_prefix}-trip-events-sub"
  project = var.project_id
  topic   = google_pubsub_topic.trip_events.id
}