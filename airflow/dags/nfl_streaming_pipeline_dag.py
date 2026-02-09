"""Airflow DAG for NFL Streaming Pipeline Management

Orchestrates:
1. Dataproc cluster management
2. Spark streaming job submission
3. Data quality checks
4. Monitoring and alerting
"""

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['alerts@nfl-analytics.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# GCP Configuration
PROJECT_ID = '{{ var.value.gcp_project_id }}'
REGION = '{{ var.value.gcp_region }}'
CLUSTER_NAME = 'nfl-streaming-cluster'
BQ_DATASET = 'nfl_analytics'
GCS_BUCKET = '{{ var.value.gcs_bucket }}'

# Dataproc cluster configuration
CLUSTER_CONFIG = {
    'master_config': {
        'num_instances': 1,
        'machine_type_uri': 'n1-standard-4',
        'disk_config': {'boot_disk_type': 'pd-standard', 'boot_disk_size_gb': 100},
    },
    'worker_config': {
        'num_instances': 3,
        'machine_type_uri': 'n1-standard-4',
        'disk_config': {'boot_disk_type': 'pd-standard', 'boot_disk_size_gb': 100},
    },
    'software_config': {
        'image_version': '2.1-debian11',
        'properties': {
            'spark:spark.jars.packages': 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0',
        },
    },
    'autoscaling_config': {
        'policy_uri': f'projects/{PROJECT_ID}/regions/{REGION}/autoscalingPolicies/nfl-autoscaling-policy'
    },
}

# Spark job configurations
PLAY_PROCESSOR_JOB = {
    'reference': {'project_id': PROJECT_ID},
    'placement': {'cluster_name': CLUSTER_NAME},
    'pyspark_job': {
        'main_python_file_uri': f'gs://{GCS_BUCKET}/spark/streaming/play_by_play_processor.py',
        'args': [
            PROJECT_ID,
            f'projects/{PROJECT_ID}/subscriptions/nfl-plays-sub',
            BQ_DATASET,
            f'gs://{GCS_BUCKET}/checkpoints/plays'
        ],
        'jar_file_uris': ['gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar'],
    },
}

TRACKING_PROCESSOR_JOB = {
    'reference': {'project_id': PROJECT_ID},
    'placement': {'cluster_name': CLUSTER_NAME},
    'pyspark_job': {
        'main_python_file_uri': f'gs://{GCS_BUCKET}/spark/streaming/player_tracking_processor.py',
        'args': [
            PROJECT_ID,
            f'projects/{PROJECT_ID}/subscriptions/nfl-tracking-sub',
            BQ_DATASET,
            f'gs://{GCS_BUCKET}/checkpoints/tracking'
        ],
        'jar_file_uris': ['gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar'],
    },
}

JOINER_JOB = {
    'reference': {'project_id': PROJECT_ID},
    'placement': {'cluster_name': CLUSTER_NAME},
    'pyspark_job': {
        'main_python_file_uri': f'gs://{GCS_BUCKET}/spark/streaming/stream_joiner.py',
        'args': [
            PROJECT_ID,
            BQ_DATASET,
            f'gs://{GCS_BUCKET}/checkpoints/joiner'
        ],
        'jar_file_uris': ['gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar'],
    },
}


with DAG(
    'nfl_streaming_pipeline',
    default_args=default_args,
    description='Manages NFL real-time streaming data pipeline',
    schedule_interval='@daily',  # Daily health check and restart if needed
    start_date=days_ago(1),
    catchup=False,
    tags=['nfl', 'streaming', 'real-time'],
) as dag:
    
    # Task 1: Create Dataproc cluster
    create_cluster = DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )
    
    # Task 2: Verify BigQuery tables exist
    check_bronze_plays_table = BigQueryTableExistenceSensor(
        task_id='check_bronze_plays_table',
        project_id=PROJECT_ID,
        dataset_id=BQ_DATASET,
        table_id='bronze_plays',
    )
    
    check_bronze_tracking_table = BigQueryTableExistenceSensor(
        task_id='check_bronze_tracking_table',
        project_id=PROJECT_ID,
        dataset_id=BQ_DATASET,
        table_id='bronze_tracking',
    )
    
    # Task 3: Submit Play-by-Play processor
    submit_play_processor = DataprocSubmitJobOperator(
        task_id='submit_play_processor',
        job=PLAY_PROCESSOR_JOB,
        region=REGION,
        project_id=PROJECT_ID,
    )
    
    # Task 4: Submit Player Tracking processor
    submit_tracking_processor = DataprocSubmitJobOperator(
        task_id='submit_tracking_processor',
        job=TRACKING_PROCESSOR_JOB,
        region=REGION,
        project_id=PROJECT_ID,
    )
    
    # Task 5: Submit Stream Joiner
    submit_stream_joiner = DataprocSubmitJobOperator(
        task_id='submit_stream_joiner',
        job=JOINER_JOB,
        region=REGION,
        project_id=PROJECT_ID,
    )
    
    # Task 6: Data quality check
    data_quality_check = BigQueryCheckOperator(
        task_id='data_quality_check',
        sql=f'''
        SELECT COUNT(*) > 0
        FROM `{PROJECT_ID}.{BQ_DATASET}.silver_plays_enriched`
        WHERE DATE(event_timestamp) = CURRENT_DATE()
        ''',
        use_legacy_sql=False,
    )
    
    # Task 7: Monitor streaming lag
    def check_streaming_lag(**context):
        """Check if streaming jobs are processing data with acceptable latency."""
        from google.cloud import bigquery
        
        client = bigquery.Client(project=PROJECT_ID)
        
        query = f"""
        SELECT 
            MAX(TIMESTAMP_DIFF(processing_timestamp, event_timestamp, SECOND)) as max_lag_seconds
        FROM `{PROJECT_ID}.{BQ_DATASET}.silver_plays_enriched`
        WHERE DATE(event_timestamp) = CURRENT_DATE()
        """
        
        result = client.query(query).result()
        for row in result:
            lag = row.max_lag_seconds
            logger.info(f"Current streaming lag: {lag} seconds")
            
            if lag > 60:  # Alert if lag > 1 minute
                raise Exception(f"Streaming lag too high: {lag} seconds")
    
    monitor_lag = PythonOperator(
        task_id='monitor_streaming_lag',
        python_callable=check_streaming_lag,
    )
    
    # Define task dependencies
    create_cluster >> [check_bronze_plays_table, check_bronze_tracking_table]
    
    check_bronze_plays_table >> submit_play_processor
    check_bronze_tracking_table >> submit_tracking_processor
    
    [submit_play_processor, submit_tracking_processor] >> submit_stream_joiner
    
    submit_stream_joiner >> data_quality_check >> monitor_lag
