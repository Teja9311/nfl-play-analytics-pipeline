# Terraform variables for NFL Analytics Pipeline

variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP region for resources"
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "GCP zone for zonal resources"
  type        = string
  default     = "us-central1-a"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "dataproc_cluster_name" {
  description = "Name of the Dataproc cluster"
  type        = string
  default     = "nfl-dataproc-cluster"
}

variable "dataproc_master_machine_type" {
  description = "Machine type for Dataproc master node"
  type        = string
  default     = "n1-standard-4"
}

variable "dataproc_worker_machine_type" {
  description = "Machine type for Dataproc worker nodes"
  type        = string
  default     = "n1-standard-4"
}

variable "dataproc_worker_count" {
  description = "Number of Dataproc worker nodes"
  type        = number
  default     = 3
}

variable "dataproc_preemptible_worker_count" {
  description = "Number of preemptible worker nodes"
  type        = number
  default     = 2
}

variable "bq_dataset_name" {
  description = "BigQuery dataset name"
  type        = string
  default     = "nfl_analytics"
}

variable "bq_dataset_location" {
  description = "BigQuery dataset location"
  type        = string
  default     = "US"
}

variable "pubsub_plays_topic" {
  description = "Pub/Sub topic for play-by-play events"
  type        = string
  default     = "nfl-plays"
}

variable "pubsub_tracking_topic" {
  description = "Pub/Sub topic for player tracking data"
  type        = string
  default     = "nfl-tracking"
}

variable "pubsub_message_retention_duration" {
  description = "Message retention duration for Pub/Sub topics"
  type        = string
  default     = "604800s"  # 7 days
}

variable "redis_memory_size_gb" {
  description = "Redis instance memory size in GB"
  type        = number
  default     = 5
}

variable "redis_tier" {
  description = "Redis service tier"
  type        = string
  default     = "BASIC"
}

variable "composer_node_count" {
  description = "Number of Cloud Composer nodes"
  type        = number
  default     = 3
}

variable "composer_machine_type" {
  description = "Machine type for Cloud Composer nodes"
  type        = string
  default     = "n1-standard-2"
}

variable "labels" {
  description = "Common labels to apply to all resources"
  type        = map(string)
  default = {
    project     = "nfl-analytics"
    managed_by  = "terraform"
    team        = "data-engineering"
  }
}
