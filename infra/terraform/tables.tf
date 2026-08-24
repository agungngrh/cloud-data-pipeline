resource "google_bigquery_table" "int_stream_trips_clean" {
  dataset_id = google_bigquery_dataset.intermediate.dataset_id
  table_id   = "int_stream_trips_clean"
  project    = var.project_id

  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "event_time"
  }

  schema = file("${path.module}/schemas/int_stream_trips_clean.json")
}


resource "google_bigquery_table" "int_stream_trips_quarantine" {
  dataset_id = google_bigquery_dataset.intermediate.dataset_id
  table_id   = "int_stream_trips_quarantine"
  project    = var.project_id

  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "event_time"
  }

  schema = file("${path.module}/schemas/int_stream_trips_quarantine.json")
}