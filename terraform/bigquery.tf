resource "google_bigquery_dataset" "raw" {
  dataset_id = "${replace(var.resource_prefix, "-", "_")}_raw"
  project    = var.project_id
  location   = var.region
}

resource "google_bigquery_dataset" "staging" {
  dataset_id = "${replace(var.resource_prefix, "-", "_")}_staging"
  project    = var.project_id
  location   = var.region
}

resource "google_bigquery_dataset" "intermediate" {
  dataset_id = "${replace(var.resource_prefix, "-", "_")}_intermediate"
  project    = var.project_id
  location   = var.region
}

resource "google_bigquery_dataset" "marts" {
  dataset_id = "${replace(var.resource_prefix, "-", "_")}_marts"
  project    = var.project_id
  location   = var.region
}