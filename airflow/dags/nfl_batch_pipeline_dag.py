"""Airflow DAG for NFL Batch Processing Pipeline

Handles:
1. Historical data aggregation
2. Feature engineering for ML models
3. Gold layer metric calculation
4. Redis cache updates
"""

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCheckOperator
)
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': True,
    'email': ['alerts@nfl-analytics.com'],
    'email_on_failure': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

PROJECT_ID = '{{ var.value.gcp_project_id }}'
REGION = '{{ var.value.gcp_region }}'
BQ_DATASET = 'nfl_analytics'
GCS_BUCKET = '{{ var.value.gcs_bucket }}'


with DAG(
    'nfl_batch_pipeline',
    default_args=default_args,
    description='Daily batch processing for NFL analytics',
    schedule_interval='0 3 * * *',  # Run daily at 3 AM
    start_date=days_ago(1),
    catchup=False,
    tags=['nfl', 'batch', 'analytics'],
) as dag:
    
    # Task 1: Calculate play effectiveness metrics
    calculate_play_effectiveness = BigQueryInsertJobOperator(
        task_id='calculate_play_effectiveness',
        configuration={
            'query': {
                'query': f'''
                CREATE OR REPLACE TABLE `{PROJECT_ID}.{BQ_DATASET}.gold_play_effectiveness`
                PARTITION BY DATE(game_date)
                CLUSTER BY possession_team, play_type
                AS
                SELECT
                    game_id,
                    play_id,
                    CAST(event_timestamp AS DATE) as game_date,
                    possession_team,
                    defensive_team,
                    play_type,
                    down,
                    yards_to_go,
                    yard_line,
                    yards_gained,
                    play_success,
                    
                    -- Expected Yards Gained (simplified model)
                    CASE 
                        WHEN play_type = 'run' THEN 4.2
                        WHEN play_type = 'pass' AND down = 1 THEN 7.5
                        WHEN play_type = 'pass' AND down = 2 THEN 6.8
                        WHEN play_type = 'pass' AND down IN (3, 4) THEN 8.2
                        ELSE 5.0
                    END as expected_yards_gained,
                    
                    -- Yards Over Expected
                    yards_gained - CASE 
                        WHEN play_type = 'run' THEN 4.2
                        WHEN play_type = 'pass' AND down = 1 THEN 7.5
                        WHEN play_type = 'pass' AND down = 2 THEN 6.8
                        WHEN play_type = 'pass' AND down IN (3, 4) THEN 8.2
                        ELSE 5.0
                    END as yards_over_expected,
                    
                    -- Success rate components
                    CASE 
                        WHEN down = 1 AND yards_gained >= yards_to_go * 0.4 THEN 1
                        WHEN down = 2 AND yards_gained >= yards_to_go * 0.6 THEN 1
                        WHEN down IN (3, 4) AND yards_gained >= yards_to_go THEN 1
                        ELSE 0
                    END as is_successful_play,
                    
                    -- Tracking metrics
                    offense_avg_speed,
                    defense_avg_speed,
                    defensive_pressure_rate,
                    avg_receiver_separation,
                    
                    -- Context
                    game_situation,
                    is_red_zone,
                    is_two_minute_drill,
                    score_differential
                    
                FROM `{PROJECT_ID}.{BQ_DATASET}.silver_plays_enriched`
                WHERE DATE(event_timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
                ''',
                'useLegacySql': False,
            }
        },
    )
    
    # Task 2: Calculate Win Probability Added (WPA)
    calculate_win_probability = BigQueryInsertJobOperator(
        task_id='calculate_win_probability',
        configuration={
            'query': {
                'query': f'''
                CREATE OR REPLACE TABLE `{PROJECT_ID}.{BQ_DATASET}.gold_win_probability`
                PARTITION BY DATE(game_date)
                CLUSTER BY game_id
                AS
                WITH play_sequences AS (
                    SELECT
                        *,
                        LAG(score_differential) OVER (PARTITION BY game_id ORDER BY event_timestamp) as prev_score_diff,
                        LAG(yard_line) OVER (PARTITION BY game_id ORDER BY event_timestamp) as prev_yard_line,
                        LEAD(score_differential) OVER (PARTITION BY game_id ORDER BY event_timestamp) as next_score_diff
                    FROM `{PROJECT_ID}.{BQ_DATASET}.gold_play_effectiveness`
                    WHERE game_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
                ),
                win_prob_calc AS (
                    SELECT
                        *,
                        -- Simplified win probability model (logistic regression coefficients)
                        1 / (1 + EXP(-(
                            0.02 * score_differential +
                            0.01 * (100 - yard_line) +
                            -0.001 * (quarter * 900 - COALESCE(time_remaining, 0)) +
                            0.3 * CASE WHEN down = 1 THEN 1 ELSE 0 END
                        ))) as win_probability_before,
                        
                        1 / (1 + EXP(-(
                            0.02 * COALESCE(next_score_diff, score_differential) +
                            0.01 * (100 - yard_line - yards_gained) +
                            -0.001 * (quarter * 900 - COALESCE(time_remaining, 0))
                        ))) as win_probability_after
                    FROM play_sequences
                )
                SELECT
                    game_id,
                    play_id,
                    game_date,
                    possession_team,
                    play_type,
                    win_probability_before,
                    win_probability_after,
                    win_probability_after - win_probability_before as win_prob_added,
                    play_success,
                    yards_gained,
                    game_situation
                FROM win_prob_calc
                ''',
                'useLegacySql': False,
            }
        },
    )
    
    # Task 3: Aggregate team statistics
    aggregate_team_stats = BigQueryInsertJobOperator(
        task_id='aggregate_team_stats',
        configuration={
            'query': {
                'query': f'''
                CREATE OR REPLACE TABLE `{PROJECT_ID}.{BQ_DATASET}.gold_team_daily_stats`
                PARTITION BY stat_date
                CLUSTER BY team
                AS
                SELECT
                    DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) as stat_date,
                    possession_team as team,
                    COUNT(*) as total_plays,
                    SUM(CASE WHEN play_type = 'run' THEN 1 ELSE 0 END) as run_plays,
                    SUM(CASE WHEN play_type = 'pass' THEN 1 ELSE 0 END) as pass_plays,
                    AVG(yards_gained) as avg_yards_per_play,
                    AVG(yards_over_expected) as avg_yards_over_expected,
                    AVG(CAST(is_successful_play AS FLOAT64)) as success_rate,
                    SUM(win_prob_added) as total_wpa,
                    AVG(offense_avg_speed) as avg_offense_speed,
                    AVG(defensive_pressure_rate) as avg_pressure_rate_faced,
                    SUM(CASE WHEN is_red_zone THEN 1 ELSE 0 END) as red_zone_plays,
                    AVG(CASE WHEN is_red_zone THEN CAST(is_successful_play AS FLOAT64) END) as red_zone_success_rate
                FROM `{PROJECT_ID}.{BQ_DATASET}.gold_play_effectiveness` pe
                JOIN `{PROJECT_ID}.{BQ_DATASET}.gold_win_probability` wp
                    ON pe.game_id = wp.game_id AND pe.play_id = wp.play_id
                WHERE pe.game_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
                GROUP BY possession_team
                ''',
                'useLegacySql': False,
            }
        },
    )
    
    # Task 4: Update Redis cache with latest metrics
    def update_redis_cache(**context):
        """Push latest team metrics to Redis for low-latency access."""
        import redis
        import json
        from google.cloud import bigquery
        
        # Connect to Redis
        redis_client = redis.Redis(
            host='{{ var.value.redis_host }}',
            port=6379,
            decode_responses=True
        )
        
        # Fetch latest team stats
        bq_client = bigquery.Client(project=PROJECT_ID)
        query = f"""
        SELECT * FROM `{PROJECT_ID}.{BQ_DATASET}.gold_team_daily_stats`
        WHERE stat_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        """
        
        results = bq_client.query(query).result()
        
        for row in results:
            team = row.team
            stats = dict(row.items())
            
            # Store in Redis with TTL of 24 hours
            redis_client.setex(
                f"nfl:team_stats:{team}",
                86400,
                json.dumps(stats, default=str)
            )
            logger.info(f"Updated Redis cache for team: {team}")
    
    update_cache = PythonOperator(
        task_id='update_redis_cache',
        python_callable=update_redis_cache,
    )
    
    # Task 5: Data quality validation
    validate_data = BigQueryCheckOperator(
        task_id='validate_gold_layer_data',
        sql=f'''
        SELECT
            COUNT(DISTINCT game_id) > 0 AND
            AVG(total_plays) > 50 AND
            AVG(success_rate) BETWEEN 0 AND 1
        FROM `{PROJECT_ID}.{BQ_DATASET}.gold_team_daily_stats`
        WHERE stat_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        ''',
        use_legacy_sql=False,
    )
    
    # Define task dependencies
    calculate_play_effectiveness >> calculate_win_probability
    calculate_win_probability >> aggregate_team_stats
    aggregate_team_stats >> [update_cache, validate_data]
