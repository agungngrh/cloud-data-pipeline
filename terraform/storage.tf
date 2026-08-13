resource "google_storage_bucket" "raw" {
  name     = "${var.project_id}-${var.resource_prefix}"
  project  = var.project_id
  location = "ASIA-SOUTHEAST2"
}