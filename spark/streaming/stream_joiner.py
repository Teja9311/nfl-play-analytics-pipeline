"""Stream Joiner for Play-by-Play and Player Tracking Data

Joins play events with aggregated player tracking metrics
to create enriched play records for win probability and effectiveness analysis.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, max, min, count, sum as spark_sum,
    when, lit, expr, broadcast, window
)
import sys
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StreamJoiner:
    """Joins play-by-play events with player tracking data."""
    
    def __init__(self, project_id, bq_dataset, checkpoint_location):
        self.project_id = project_id
        self.bq_dataset = bq_dataset
        self.checkpoint_location = checkpoint_location
        self.spark = self._create_spark_session()
    
    def _create_spark_session(self):
        """Create Spark session."""
        return SparkSession.builder \
            .appName("NFL-Stream-Joiner") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .getOrCreate()
    
    def read_plays_stream(self):
        """Read play-by-play data from BigQuery."""
        return self.spark \
            .readStream \
            .format("bigquery") \
            .option("table", f"{self.project_id}:{self.bq_dataset}.bronze_plays") \
            .load()
    
    def read_tracking_stream(self):
        """Read player tracking data from BigQuery."""
        return self.spark \
            .readStream \
            .format("bigquery") \
            .option("table", f"{self.project_id}:{self.bq_dataset}.bronze_tracking") \
            .load()
    
    def aggregate_tracking_by_play(self, tracking_df):
        """Aggregate tracking metrics per play."""
        # Offensive players aggregation
        offense_agg = tracking_df \
            .filter(col("team") == col("possession_team")) \
            .groupBy("game_id", "play_id") \
            .agg(
                avg("final_speed").alias("offense_avg_speed"),
                max("final_speed").alias("offense_max_speed"),
                avg("calculated_acceleration").alias("offense_avg_acceleration"),
                spark_sum("distance_traveled").alias("offense_total_distance")
            )
        
        # Defensive players aggregation
        defense_agg = tracking_df \
            .filter(col("team") == col("defensive_team")) \
            .groupBy("game_id", "play_id") \
            .agg(
                avg("final_speed").alias("defense_avg_speed"),
                max("final_speed").alias("defense_max_speed"),
                avg("calculated_acceleration").alias("defense_avg_acceleration"),
                spark_sum("distance_traveled").alias("defense_total_distance")
            )
        
        # QB-specific metrics
        qb_metrics = tracking_df \
            .filter(col("position") == "QB") \
            .groupBy("game_id", "play_id") \
            .agg(
                avg("final_speed").alias("qb_avg_speed"),
                max("final_speed").alias("qb_max_speed"),
                avg(when(col("event") == "pass", col("calculated_acceleration"))).alias("qb_pressure_indicator")
            )
        
        # Receiver separation metrics (distance from nearest defender)
        receiver_metrics = tracking_df \
            .filter(col("position").isin(["WR", "TE", "RB"])) \
            .filter(col("event") == "pass_arrived") \
            .groupBy("game_id", "play_id") \
            .agg(
                avg("distance_traveled").alias("avg_receiver_separation")
            )
        
        return offense_agg, defense_agg, qb_metrics, receiver_metrics
    
    def calculate_pressure_rate(self, tracking_df):
        """Calculate defensive pressure rate on QB."""
        # Count defenders within 2 yards of QB at pass release
        pressure_df = tracking_df \
            .filter(col("event") == "pass") \
            .groupBy("game_id", "play_id") \
            .agg(
                count(
                    when(
                        (col("position").isin(["DE", "DT", "LB", "DB"])) &
                        (col("distance_traveled") < 2),
                        1
                    )
                ).alias("pressure_count"),
                count("player_id").alias("total_defenders")
            ) \
            .withColumn(
                "defensive_pressure_rate",
                col("pressure_count") / col("total_defenders")
            )
        
        return pressure_df
    
    def join_streams(self, plays_df, tracking_df):
        """Join play events with aggregated tracking metrics."""
        # Aggregate tracking data
        offense_agg, defense_agg, qb_metrics, receiver_metrics = \
            self.aggregate_tracking_by_play(tracking_df)
        
        # Calculate pressure
        pressure_df = self.calculate_pressure_rate(tracking_df)
        
        # Join all streams
        # Use watermark for late data handling (10 minutes)
        plays_with_watermark = plays_df.withWatermark("event_timestamp", "10 minutes")
        tracking_with_watermark = offense_agg.withWatermark("event_timestamp", "10 minutes")
        
        joined_df = plays_with_watermark \
            .join(
                offense_agg,
                ["game_id", "play_id"],
                "left"
            ) \
            .join(
                defense_agg,
                ["game_id", "play_id"],
                "left"
            ) \
            .join(
                qb_metrics,
                ["game_id", "play_id"],
                "left"
            ) \
            .join(
                receiver_metrics,
                ["game_id", "play_id"],
                "left"
            ) \
            .join(
                pressure_df,
                ["game_id", "play_id"],
                "left"
            )
        
        return joined_df
    
    def calculate_win_probability_features(self, df):
        """Calculate features for win probability model."""
        wp_df = df \
            .withColumn(
                "seconds_elapsed",
                (4 - col("quarter")) * 900 + col("time_remaining")
            ) \
            .withColumn(
                "is_offense_leading",
                when(col("score_differential") > 0, 1).otherwise(0)
            ) \
            .withColumn(
                "field_position_value",
                # Better field position = closer to opponent end zone
                100 - col("yard_line")
            ) \
            .withColumn(
                "down_distance_ratio",
                when(col("yards_to_go") > 0, col("down") / col("yards_to_go")).otherwise(0)
            )
        
        return wp_df
    
    def write_to_silver_layer(self, df):
        """Write joined data to silver layer."""
        query = df \
            .writeStream \
            .format("bigquery") \
            .option("table", f"{self.project_id}:{self.bq_dataset}.silver_plays_enriched") \
            .option("checkpointLocation", f"{self.checkpoint_location}/silver_plays") \
            .option("failOnDataLoss", "false") \
            .outputMode("append") \
            .trigger(processingTime="10 seconds")
        
        return query
    
    def run(self):
        """Run the stream joining pipeline."""
        try:
            logger.info("Starting Stream Joiner...")
            
            # Read streams
            plays_stream = self.read_plays_stream()
            tracking_stream = self.read_tracking_stream()
            
            # Join streams
            joined_stream = self.join_streams(plays_stream, tracking_stream)
            
            # Calculate win probability features
            enriched_stream = self.calculate_win_probability_features(joined_stream)
            
            # Write to silver layer
            query = self.write_to_silver_layer(enriched_stream).start()
            
            logger.info("Stream joiner started successfully")
            
            query.awaitTermination()
            
        except Exception as e:
            logger.error(f"Error in stream joiner: {str(e)}")
            raise
        finally:
            self.spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: stream_joiner.py <project_id> <bq_dataset> <checkpoint_location>")
        sys.exit(1)
    
    project_id = sys.argv[1]
    bq_dataset = sys.argv[2]
    checkpoint_location = sys.argv[3]
    
    joiner = StreamJoiner(
        project_id=project_id,
        bq_dataset=bq_dataset,
        checkpoint_location=checkpoint_location
    )
    
    joiner.run()
