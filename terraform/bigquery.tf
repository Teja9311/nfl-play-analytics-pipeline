# BigQuery configuration for NFL Analytics

# Main dataset
resource "google_bigquery_dataset" "nfl_analytics" {
  dataset_id  = var.bq_dataset_name
  location    = var.bq_dataset_location
  description = "NFL play effectiveness and win probability analytics"
  
  labels = var.labels
  
  default_table_expiration_ms = null  # Tables don't expire
  
  access {
    role          = "OWNER"
    user_by_email = google_service_account.dataproc_sa.email
  }
}

# Bronze layer - Raw plays
resource "google_bigquery_table" "bronze_plays" {
  dataset_id = google_bigquery_dataset.nfl_analytics.dataset_id
  table_id   = "bronze_plays"
  
  deletion_protection = true
  
  time_partitioning {
    type  = "DAY"
    field = "event_timestamp"
  }
  
  clustering = ["game_id", "possession_team"]
  
  schema = jsonencode([
    {"name": "game_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "play_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "event_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "possession_team", "type": "STRING", "mode": "REQUIRED"},
    {"name": "defensive_team", "type": "STRING", "mode": "REQUIRED"},
    {"name": "down", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "yards_to_go", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "yard_line", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "quarter", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "time_remaining", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "play_type", "type": "STRING", "mode": "NULLABLE"},
    {"name": "play_result", "type": "STRING", "mode": "NULLABLE"},
    {"name": "yards_gained", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "is_touchdown", "type": "BOOLEAN", "mode": "NULLABLE"},
    {"name": "is_turnover", "type": "BOOLEAN", "mode": "NULLABLE"},
    {"name": "is_penalty", "type": "BOOLEAN", "mode": "NULLABLE"},
    {"name": "penalty_yards", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "score_home", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "score_away", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "formation", "type": "STRING", "mode": "NULLABLE"},
    {"name": "personnel_offense", "type": "STRING", "mode": "NULLABLE"},
    {"name": "personnel_defense", "type": "STRING", "mode": "NULLABLE"},
    {"name": "processing_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "is_valid", "type": "BOOLEAN", "mode": "REQUIRED"},
    {"name": "is_red_zone", "type": "BOOLEAN", "mode": "NULLABLE"},
    {"name": "is_two_minute_drill", "type": "BOOLEAN", "mode": "NULLABLE"},
    {"name": "play_success", "type": "BOOLEAN", "mode": "NULLABLE"},
    {"name": "score_differential", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "game_situation", "type": "STRING", "mode": "NULLABLE"}
  ])
  
  labels = merge(var.labels, {
    layer = "bronze"
  })
}

# Bronze layer - Dead letter queue for plays
resource "google_bigquery_table" "bronze_plays_dlq" {
  dataset_id = google_bigquery_dataset.nfl_analytics.dataset_id
  table_id   = "bronze_plays_dlq"
  
  deletion_protection = false
  
  time_partitioning {
    type  = "DAY"
    field = "processing_timestamp"
  }
  
  schema = jsonencode([
    {"name": "game_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "play_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "raw_data", "type": "STRING", "mode": "NULLABLE"},
    {"name": "error_message", "type": "STRING", "mode": "NULLABLE"},
    {"name": "processing_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"}
  ])
  
  labels = merge(var.labels, {
    layer = "bronze"
    type  = "dlq"
  })
}

# Bronze layer - Player tracking
resource "google_bigquery_table" "bronze_tracking" {
  dataset_id = google_bigquery_dataset.nfl_analytics.dataset_id
  table_id   = "bronze_tracking"
  
  deletion_protection = true
  
  time_partitioning {
    type  = "DAY"
    field = "timestamp"
  }
  
  clustering = ["game_id", "play_id", "player_id"]
  
  schema = jsonencode([
    {"name": "game_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "play_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "frame_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "player_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "team", "type": "STRING", "mode": "REQUIRED"},
    {"name": "jersey_number", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "position", "type": "STRING", "mode": "NULLABLE"},
    {"name": "x_coord", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "y_coord", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "final_speed", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "calculated_acceleration", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "direction", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "orientation", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "event", "type": "STRING", "mode": "NULLABLE"},
    {"name": "distance_traveled", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "processing_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"}
  ])
  
  labels = merge(var.labels, {
    layer = "bronze"
  })
}

# Silver layer - Enriched plays
resource "google_bigquery_table" "silver_plays_enriched" {
  dataset_id = google_bigquery_dataset.nfl_analytics.dataset_id
  table_id   = "silver_plays_enriched"
  
  deletion_protection = true
  
  time_partitioning {
    type  = "DAY"
    field = "event_timestamp"
  }
  
  clustering = ["game_id", "possession_team", "play_type"]
  
  labels = merge(var.labels, {
    layer = "silver"
  })
}

# Outputs
output "bigquery_dataset" {
  value       = google_bigquery_dataset.nfl_analytics.dataset_id
  description = "BigQuery dataset for NFL analytics"
}
