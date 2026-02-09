"""Historical Data Aggregator

Runs nightly to aggregate past game data into clean summaries.
Used for building features for ML models and backfilling historical trends.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, sum as spark_sum, count, max, min,
    when, lag, lead, row_number, dense_rank
)
from pyspark.sql.window import Window
import sys
import logging
from datetime import datetime, timedelta

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HistoricalAggregator:
    """Batch job to aggregate historical play and tracking data."""
    
    def __init__(self, project_id, bq_dataset, start_date, end_date):
        self.project_id = project_id
        self.bq_dataset = bq_dataset
        self.start_date = start_date
        self.end_date = end_date
        self.spark = self._create_spark_session()
    
    def _create_spark_session(self):
        """Spin up Spark with BigQuery connector."""
        return SparkSession.builder \
            .appName("NFL-Historical-Aggregator") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
    
    def load_silver_plays(self):
        """Pull enriched play data from silver layer."""
        logger.info(f"Loading plays from {self.start_date} to {self.end_date}")
        
        query = f"""
        SELECT *
        FROM `{self.project_id}.{self.bq_dataset}.silver_plays_enriched`
        WHERE DATE(event_timestamp) BETWEEN '{self.start_date}' AND '{self.end_date}'
        """
        
        return self.spark.read.format("bigquery").option("query", query).load()
    
    def aggregate_by_game(self, df):
        """Roll up play stats per game."""
        game_stats = df.groupBy("game_id", "possession_team").agg(
            count("*").alias("total_plays"),
            spark_sum("yards_gained").alias("total_yards"),
            avg("yards_gained").alias("avg_yards_per_play"),
            spark_sum(when(col("play_type") == "run", 1).otherwise(0)).alias("run_plays"),
            spark_sum(when(col("play_type") == "pass", 1).otherwise(0)).alias("pass_plays"),
            spark_sum(when(col("is_touchdown"), 1).otherwise(0)).alias("touchdowns"),
            spark_sum(when(col("is_turnover"), 1).otherwise(0)).alias("turnovers"),
            avg(when(col("play_success"), 1.0).otherwise(0.0)).alias("success_rate"),
            # Tracking-derived stats
            avg("offense_avg_speed").alias("avg_offense_speed"),
            avg("defensive_pressure_rate").alias("avg_pressure_faced"),
            avg("avg_receiver_separation").alias("avg_separation"),
            # Situational breakdowns
            avg(when(col("is_red_zone"), 1.0).otherwise(0.0)).alias("red_zone_pct"),
            avg(when(col("down") == 3, 1.0).otherwise(0.0)).alias("third_down_attempts")
        )
        
        return game_stats
    
    def aggregate_by_week(self, df):
        """Weekly team aggregates for trend analysis."""
        # Extract week number from event timestamp
        df_with_week = df.withColumn(
            "week_num",
            ((col("event_timestamp").cast("long") / 86400) % 7)
        )
        
        weekly_stats = df_with_week.groupBy("week_num", "possession_team").agg(
            count("*").alias("total_plays"),
            avg("yards_gained").alias("avg_yards_per_play"),
            avg(when(col("play_success"), 1.0).otherwise(0.0)).alias("success_rate"),
            avg("offense_avg_speed").alias("avg_offense_speed")
        )
        
        return weekly_stats
    
    def calculate_team_rankings(self, game_stats):
        """Rank teams by key metrics for comparison."""
        window = Window.orderBy(col("avg_yards_per_play").desc())
        
        ranked = game_stats.withColumn(
            "yards_per_play_rank",
            dense_rank().over(window)
        )
        
        window_success = Window.orderBy(col("success_rate").desc())
        ranked = ranked.withColumn(
            "success_rate_rank",
            dense_rank().over(window_success)
        )
        
        return ranked
    
    def calculate_moving_averages(self, weekly_stats):
        """Add 3-week moving averages to spot trends."""
        window = Window.partitionBy("possession_team").orderBy("week_num").rowsBetween(-2, 0)
        
        ma_stats = weekly_stats \
            .withColumn("ma3_yards_per_play", avg("avg_yards_per_play").over(window)) \
            .withColumn("ma3_success_rate", avg("success_rate").over(window))
        
        return ma_stats
    
    def write_to_gold(self, df, table_name):
        """Write aggregated data to gold layer."""
        logger.info(f"Writing to gold table: {table_name}")
        
        df.write \
            .format("bigquery") \
            .option("table", f"{self.project_id}:{self.bq_dataset}.{table_name}") \
            .option("writeMethod", "direct") \
            .mode("overwrite") \
            .save()
        
        logger.info(f"Successfully wrote {df.count()} records to {table_name}")
    
    def run(self):
        """Main execution flow."""
        try:
            logger.info("Starting historical aggregation job...")
            
            # Load silver data
            plays_df = self.load_silver_plays()
            
            # Game-level aggregations
            game_stats = self.aggregate_by_game(plays_df)
            ranked_game_stats = self.calculate_team_rankings(game_stats)
            self.write_to_gold(ranked_game_stats, "gold_game_aggregates")
            
            # Weekly aggregations
            weekly_stats = self.aggregate_by_week(plays_df)
            weekly_with_ma = self.calculate_moving_averages(weekly_stats)
            self.write_to_gold(weekly_with_ma, "gold_weekly_aggregates")
            
            logger.info("Historical aggregation completed successfully")
            
        except Exception as e:
            logger.error(f"Error in historical aggregation: {str(e)}")
            raise
        finally:
            self.spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: historical_aggregator.py <project_id> <bq_dataset> <start_date> <end_date>")
        print("Example: historical_aggregator.py my-project nfl_analytics 2024-09-01 2024-09-30")
        sys.exit(1)
    
    project_id = sys.argv[1]
    bq_dataset = sys.argv[2]
    start_date = sys.argv[3]
    end_date = sys.argv[4]
    
    aggregator = HistoricalAggregator(
        project_id=project_id,
        bq_dataset=bq_dataset,
        start_date=start_date,
        end_date=end_date
    )
    
    aggregator.run()
