terraform {
  backend "gcs" {
    bucket = "terraform-state-jcdeah-009"
    prefix = "terraform/state"
  }
}