resource "google_bigquery_dataset" "raw" {
  dataset_id                 = "${replace(var.resource_prefix, "-", "_")}_raw"
  project                    = var.project_id
  location                   = var.region
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "staging" {
  dataset_id                 = "${replace(var.resource_prefix, "-", "_")}_staging"
  project                    = var.project_id
  location                   = var.region
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "intermediate" {
  dataset_id                 = "${replace(var.resource_prefix, "-", "_")}_intermediate"
  project                    = var.project_id
  location                   = var.region
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "marts" {
  dataset_id                 = "${replace(var.resource_prefix, "-", "_")}_marts"
  project                    = var.project_id
  location                   = var.region
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "ops" {
  dataset_id                 = "${replace(var.resource_prefix, "-", "_")}_ops"
  project                    = var.project_id
  location                   = var.region
  delete_contents_on_destroy = true
}

resource "google_bigquery_table" "pipeline_run_log" {
  dataset_id          = google_bigquery_dataset.ops.dataset_id
  table_id            = "pipeline_run_log"
  project             = var.project_id
  deletion_protection = false
  schema              = file("${path.module}/schemas/pipeline_run_log.json")
}

resource "google_bigquery_dataset" "ci" {
  dataset_id                 = "${replace(var.resource_prefix, "-", "_")}_ci"
  project                    = var.project_id
  location                   = var.region
  delete_contents_on_destroy = true
}