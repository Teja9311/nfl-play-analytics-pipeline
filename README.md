# 🏈 NFL Play Effectiveness & Win Probability Analytics Pipeline

## Overview

A real-time data engineering pipeline that ingests NFL play-by-play and player-tracking streams, processes them using Apache Spark on GCP, and produces win-probability and play-effectiveness metrics for coaches, analysts, and live broadcasts.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     DATA SOURCES                                 │
├─────────────────────────────────────────────────────────────────┤
│  • Play-by-Play Events (Pub/Sub)                                │
│  • Player Tracking Data (Pub/Sub)                               │
│  • Game Context (Cloud Storage)                                  │
│  • Historical Data (BigQuery)                                    │
└─────────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────────┐
│              STREAM PROCESSING (Dataproc/Spark)                  │
├─────────────────────────────────────────────────────────────────┤
│  • Structured Streaming Jobs                                     │
│  • Real-time Joins (plays + tracking)                           │
│  • Data Quality Checks                                           │
│  • Feature Engineering                                           │
└─────────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────────┐
│                   DATA LAYERS (BigQuery)                         │
├─────────────────────────────────────────────────────────────────┤
│  Bronze  → Raw events (plays, tracking)                         │
│  Silver  → Cleaned & joined data                                │
│  Gold    → Aggregated metrics (WPA, success rate)               │
└─────────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────────┐
│                    SERVING LAYERS                                │
├─────────────────────────────────────────────────────────────────┤
│  • Memorystore (Redis) - Real-time dashboards                   │
│  • BigQuery - Analytics & ML features                            │
│  • Cloud Storage - Data Lake                                     │
└─────────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────────┐
│                      CONSUMERS                                   │
├─────────────────────────────────────────────────────────────────┤
│  • Coaching Dashboards (Looker/Tableau)                         │
│  • ML Models (Vertex AI)                                         │
│  • Broadcast Graphics APIs                                       │
│  • Fan Apps                                                      │
└─────────────────────────────────────────────────────────────────┘
```

## Tech Stack

- **Cloud Platform**: Google Cloud Platform (GCP)
- **Stream Processing**: Apache Spark (Dataproc), PySpark Structured Streaming
- **Orchestration**: Cloud Composer (Managed Airflow)
- **Message Queue**: Cloud Pub/Sub
- **Data Warehouse**: BigQuery
- **Caching**: Memorystore for Redis
- **Storage**: Cloud Storage (Data Lake)
- **Monitoring**: Cloud Monitoring, Cloud Logging
- **IaC**: Terraform
- **Language**: Python 3.11+

## Project Structure

```
nfl-play-analytics-pipeline/
│
├── airflow/
│   ├── dags/
│   │   ├── nfl_streaming_pipeline_dag.py
│   │   ├── nfl_batch_pipeline_dag.py
│   │   └── data_quality_dag.py
│   └── plugins/
│       └── custom_operators.py
│
├── spark/
│   ├── streaming/
│   │   ├── play_by_play_processor.py
│   │   ├── player_tracking_processor.py
│   │   └── stream_joiner.py
│   ├── batch/
│   │   ├── historical_aggregator.py
│   │   └── feature_engineering.py
│   └── utils/
│       ├── data_quality.py
│       ├── schema.py
│       └── transformations.py
│
├── data_generators/
│   ├── play_event_simulator.py
│   ├── tracking_data_simulator.py
│   └── game_context_generator.py
│
├── config/
│   ├── dataproc_config.yaml
│   ├── bigquery_schemas.json
│   └── pipeline_config.yaml
│
├── terraform/
│   ├── main.tf
│   ├── variables.tf
│   ├── pubsub.tf
│   ├── dataproc.tf
│   ├── bigquery.tf
│   └── composer.tf
│
├── sql/
│   ├── create_tables.sql
│   ├── gold_layer_views.sql
│   └── analytics_queries.sql
│
├── tests/
│   ├── test_transformations.py
│   ├── test_data_quality.py
│   └── test_streaming.py
│
├── requirements.txt
├── setup.py
└── README.md
```

## Core Metrics

### 1. Expected Yards Gained (EYG)
Predicted yards based on down, distance, field position, and historical data.

### 2. Play Success Rate
Percentage of plays that gain:
- 40%+ of yards needed on 1st down
- 60%+ of yards needed on 2nd down
- 100%+ of yards needed on 3rd/4th down

### 3. Win Probability Added (WPA)
Change in win probability before and after each play.

### 4. Defensive Pressure Rate
Frequency of defenders within 2 yards of QB at pass release.

### 5. Separation at Catch Point
Distance between receiver and nearest defender at catch.

## Setup Instructions

### Prerequisites

1. **GCP Account** with billing enabled
2. **GCP Project** created
3. **gcloud CLI** installed and configured
4. **Terraform** installed (v1.5+)
5. **Python** 3.11+

### Step 1: Clone Repository

```bash
git clone https://github.com/Teja9311/nfl-play-analytics-pipeline.git
cd nfl-play-analytics-pipeline
```

### Step 2: Set Environment Variables

```bash
export GCP_PROJECT_ID="your-project-id"
export GCP_REGION="us-central1"
export GCS_BUCKET="${GCP_PROJECT_ID}-nfl-data"
```

### Step 3: Infrastructure Deployment

```bash
cd terraform
terraform init
terraform plan -var="project_id=$GCP_PROJECT_ID" -var="region=$GCP_REGION"
terraform apply -var="project_id=$GCP_PROJECT_ID" -var="region=$GCP_REGION"
```

### Step 4: Install Python Dependencies

```bash
pip install -r requirements.txt
```

### Step 5: Deploy Airflow DAGs

```bash
# Get Composer environment details
gcloud composer environments describe nfl-composer-env \
    --location $GCP_REGION \
    --format="get(config.dagGcsPrefix)"

# Upload DAGs
gsutil -m cp -r airflow/dags/* gs://<composer-bucket>/dags/
```

### Step 6: Submit Spark Jobs

```bash
# Start streaming job
gcloud dataproc jobs submit pyspark \
    spark/streaming/play_by_play_processor.py \
    --cluster=nfl-dataproc-cluster \
    --region=$GCP_REGION \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar
```

### Step 7: Start Data Simulators (for testing)

```bash
python data_generators/play_event_simulator.py
python data_generators/tracking_data_simulator.py
```

## Data Flow

### Real-Time Path

1. **Ingestion**: Events published to Pub/Sub topics
2. **Stream Processing**: Spark Structured Streaming reads from Pub/Sub
3. **Transformation**: Clean, validate, join, enrich data
4. **Storage**: Write to BigQuery (bronze → silver → gold)
5. **Caching**: Push latest metrics to Redis
6. **Consumption**: APIs, dashboards, ML models

### Batch Path

1. **Ingestion**: Historical data loaded from Cloud Storage
2. **Processing**: Spark batch jobs aggregate metrics
3. **Feature Engineering**: Create ML features
4. **Storage**: Write to BigQuery gold layer
5. **Consumption**: Analytics, reporting, model training

## Key Features

### Data Quality
- Schema validation using Great Expectations
- Anomaly detection (impossible speeds, missing coordinates)
- Late data handling with watermarking
- Duplicate detection and deduplication

### Scalability
- Autoscaling Dataproc clusters
- Partitioned BigQuery tables (by game_id, date)
- Streaming inserts with batching
- Horizontal scaling for multiple concurrent games

### Monitoring
- Custom Cloud Monitoring dashboards
- Alerting on SLA violations
- Data freshness checks
- Pipeline performance metrics

## Sample Queries

### Win Probability by Play Type

```sql
SELECT 
  play_type,
  AVG(win_prob_added) as avg_wpa,
  COUNT(*) as play_count
FROM `project.nfl_gold.play_effectiveness`
WHERE season = 2025
  AND quarter <= 4
GROUP BY play_type
ORDER BY avg_wpa DESC;
```

### Top Pressure Situations

```sql
SELECT 
  game_id,
  play_id,
  qb_pressure_rate,
  sack_probability,
  play_result
FROM `project.nfl_silver.plays_with_tracking`
WHERE qb_pressure_rate > 0.6
ORDER BY qb_pressure_rate DESC
LIMIT 100;
```

## Performance Benchmarks

- **Latency**: < 2 seconds from event to dashboard
- **Throughput**: 10K events/sec per game
- **Concurrent Games**: Up to 16 games simultaneously
- **Data Freshness**: Real-time (< 1 second lag)

## Cost Optimization

- Use preemptible workers for Dataproc (60% cost savings)
- BigQuery partitioning and clustering
- Pub/Sub message retention = 7 days
- Autoscaling policies for compute resources

## Contributing

Pull requests welcome! Please follow:
1. Fork the repository
2. Create a feature branch
3. Write tests for new functionality
4. Submit PR with detailed description

