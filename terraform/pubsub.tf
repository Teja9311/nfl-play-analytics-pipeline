# Pub/Sub configuration for NFL streaming data

# Topic for play-by-play events
resource "google_pubsub_topic" "plays" {
  name = var.pubsub_plays_topic
  
  labels = merge(var.labels, {
    data_type = "play-by-play"
  })
  
  message_retention_duration = var.pubsub_message_retention_duration
}

# Subscription for play-by-play events
resource "google_pubsub_subscription" "plays_sub" {
  name  = "${var.pubsub_plays_topic}-sub"
  topic = google_pubsub_topic.plays.name
  
  # Acknowledge deadline
  ack_deadline_seconds = 60
  
  # Message retention
  message_retention_duration = var.pubsub_message_retention_duration
  
  # Retry policy
  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
  
  # Dead letter policy
  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.plays_dlq.id
    max_delivery_attempts = 5
  }
  
  # Enable exactly-once delivery
  enable_exactly_once_delivery = true
  
  labels = var.labels
}

# Dead letter topic for plays
resource "google_pubsub_topic" "plays_dlq" {
  name = "${var.pubsub_plays_topic}-dlq"
  
  labels = merge(var.labels, {
    data_type = "dead-letter"
  })
}

resource "google_pubsub_subscription" "plays_dlq_sub" {
  name  = "${var.pubsub_plays_topic}-dlq-sub"
  topic = google_pubsub_topic.plays_dlq.name
  
  message_retention_duration = "604800s"  # 7 days
}

# Topic for player tracking data
resource "google_pubsub_topic" "tracking" {
  name = var.pubsub_tracking_topic
  
  labels = merge(var.labels, {
    data_type = "player-tracking"
  })
  
  message_retention_duration = var.pubsub_message_retention_duration
}

# Subscription for player tracking
resource "google_pubsub_subscription" "tracking_sub" {
  name  = "${var.pubsub_tracking_topic}-sub"
  topic = google_pubsub_topic.tracking.name
  
  ack_deadline_seconds       = 60
  message_retention_duration = var.pubsub_message_retention_duration
  
  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
  
  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.tracking_dlq.id
    max_delivery_attempts = 5
  }
  
  enable_exactly_once_delivery = true
  
  labels = var.labels
}

# Dead letter topic for tracking
resource "google_pubsub_topic" "tracking_dlq" {
  name = "${var.pubsub_tracking_topic}-dlq"
  
  labels = merge(var.labels, {
    data_type = "dead-letter"
  })
}

resource "google_pubsub_subscription" "tracking_dlq_sub" {
  name  = "${var.pubsub_tracking_topic}-dlq-sub"
  topic = google_pubsub_topic.tracking_dlq.name
  
  message_retention_duration = "604800s"
}

# IAM binding for Pub/Sub publisher (for data generators)
resource "google_pubsub_topic_iam_member" "plays_publisher" {
  topic  = google_pubsub_topic.plays.name
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${google_service_account.dataproc_sa.email}"
}

resource "google_pubsub_topic_iam_member" "tracking_publisher" {
  topic  = google_pubsub_topic.tracking.name
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${google_service_account.dataproc_sa.email}"
}

# Outputs
output "plays_topic" {
  value       = google_pubsub_topic.plays.name
  description = "Pub/Sub topic for play-by-play events"
}

output "plays_subscription" {
  value       = google_pubsub_subscription.plays_sub.name
  description = "Pub/Sub subscription for play-by-play events"
}

output "tracking_topic" {
  value       = google_pubsub_topic.tracking.name
  description = "Pub/Sub topic for tracking data"
}

output "tracking_subscription" {
  value       = google_pubsub_subscription.tracking_sub.name
  description = "Pub/Sub subscription for tracking data"
}
