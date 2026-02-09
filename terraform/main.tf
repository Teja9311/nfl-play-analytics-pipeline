# Main Terraform configuration for NFL Analytics Pipeline
# Provisions all required GCP resources

terraform {
  required_version = ">= 1.5.0"
  
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
  
  backend "gcs" {
    bucket = "nfl-terraform-state"
    prefix = "terraform/state"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Enable required APIs
resource "google_project_service" "required_apis" {
  for_each = toset([
    "dataproc.googleapis.com",
    "bigquery.googleapis.com",
    "pubsub.googleapis.com",
    "storage.googleapis.com",
    "composer.googleapis.com",
    "redis.googleapis.com",
    "monitoring.googleapis.com",
    "logging.googleapis.com"
  ])
  
  service            = each.value
  disable_on_destroy = false
}

# Create GCS bucket for data lake
resource "google_storage_bucket" "data_lake" {
  name          = "${var.project_id}-nfl-data-lake"
  location      = var.region
  force_destroy = false
  
  uniform_bucket_level_access = true
  
  versioning {
    enabled = true
  }
  
  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  
  lifecycle_rule {
    condition {
      age = 365
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }
}

# Create folders in GCS
resource "google_storage_bucket_object" "folders" {
  for_each = toset([
    "raw/plays/",
    "raw/tracking/",
    "raw/game_context/",
    "spark/streaming/",
    "spark/batch/",
    "checkpoints/plays/",
    "checkpoints/tracking/",
    "checkpoints/joiner/",
    "temp/"
  ])
  
  name    = each.value
  content = "placeholder"
  bucket  = google_storage_bucket.data_lake.name
}

# Service account for Dataproc
resource "google_service_account" "dataproc_sa" {
  account_id   = "nfl-dataproc-sa"
  display_name = "NFL Dataproc Service Account"
  description  = "Service account for Dataproc clusters running NFL analytics"
}

# IAM bindings for service account
resource "google_project_iam_member" "dataproc_worker" {
  project = var.project_id
  role    = "roles/dataproc.worker"
  member  = "serviceAccount:${google_service_account.dataproc_sa.email}"
}

resource "google_project_iam_member" "bigquery_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.dataproc_sa.email}"
}

resource "google_project_iam_member" "storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.dataproc_sa.email}"
}

resource "google_project_iam_member" "pubsub_subscriber" {
  project = var.project_id
  role    = "roles/pubsub.subscriber"
  member  = "serviceAccount:${google_service_account.dataproc_sa.email}"
}

# Output important values
output "data_lake_bucket" {
  value       = google_storage_bucket.data_lake.name
  description = "GCS bucket for data lake"
}

output "dataproc_service_account" {
  value       = google_service_account.dataproc_sa.email
  description = "Service account email for Dataproc"
}
