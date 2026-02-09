"""Real-time Player Tracking Data Processor

Processes high-frequency player position data (10-20 Hz),
applies data quality checks, and writes to BigQuery.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, current_timestamp, window,
    sqrt, pow, lag, lead, avg, stddev,
    when, lit, unix_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    FloatType, TimestampType, ArrayType
)
from pyspark.sql.window import Window
import sys
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PlayerTrackingProcessor:
    """Processes real-time player tracking data."""
    
    def __init__(self, project_id, pubsub_subscription, bq_dataset, checkpoint_location):
        self.project_id = project_id
        self.pubsub_subscription = pubsub_subscription
        self.bq_dataset = bq_dataset
        self.checkpoint_location = checkpoint_location
        self.spark = self._create_spark_session()
    
    def _create_spark_session(self):
        """Create Spark session with optimized configs for high-frequency data."""
        return SparkSession.builder \
            .appName("NFL-PlayerTracking-Processor") \
            .config("spark.sql.streaming.schemaInference", "true") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .config("spark.sql.shuffle.partitions", "200") \
            .getOrCreate()
    
    def get_tracking_schema(self):
        """Define schema for player tracking data."""
        return StructType([
            StructField("game_id", StringType(), False),
            StructField("play_id", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("frame_id", IntegerType(), False),
            StructField("player_id", StringType(), False),
            StructField("team", StringType(), False),
            StructField("jersey_number", IntegerType(), True),
            StructField("position", StringType(), True),
            StructField("x_coord", FloatType(), False),  # yards from left sideline
            StructField("y_coord", FloatType(), False),  # yards from own goal line
            StructField("speed", FloatType(), True),  # yards per second
            StructField("acceleration", FloatType(), True),  # yards per second squared
            StructField("direction", FloatType(), True),  # degrees (0-360)
            StructField("orientation", FloatType(), True),  # body facing direction
            StructField("event", StringType(), True)  # snap, pass, tackle, etc.
        ])
    
    def read_from_pubsub(self):
        """Read high-frequency tracking data from Pub/Sub."""
        logger.info(f"Reading tracking data from: {self.pubsub_subscription}")
        
        return self.spark \
            .readStream \
            .format("pubsub") \
            .option("pubsub.subscription", self.pubsub_subscription) \
            .option("pubsub.project", self.project_id) \
            .load()
    
    def parse_and_validate(self, df):
        """Parse JSON and validate tracking data."""
        schema = self.get_tracking_schema()
        
        parsed_df = df.select(
            from_json(col("data").cast("string"), schema).alias("tracking")
        ).select("tracking.*")
        
        # Data quality checks for tracking data
        validated_df = parsed_df \
            .withColumn("processing_timestamp", current_timestamp()) \
            .withColumn(
                "is_valid",
                when(
                    (col("x_coord").between(0, 53.3)) &  # Field width
                    (col("y_coord").between(0, 120)) &    # Field length + end zones
                    (col("speed").between(0, 25)) &       # Max human speed ~23 mph = 33.7 ft/s
                    (col("direction").between(0, 360)) &
                    (col("orientation").between(0, 360)),
                    True
                ).otherwise(False)
            ) \
            .withColumn(
                "is_outlier_speed",
                when(col("speed") > 22, True).otherwise(False)  # Flag impossibly fast speeds
            )
        
        return validated_df
    
    def calculate_derived_metrics(self, df):
        """Calculate speed, acceleration, and distance metrics."""
        # Define window for player tracking over time
        window_spec = Window.partitionBy("game_id", "play_id", "player_id").orderBy("timestamp")
        
        derived_df = df \
            .withColumn("prev_x", lag("x_coord", 1).over(window_spec)) \
            .withColumn("prev_y", lag("y_coord", 1).over(window_spec)) \
            .withColumn("prev_timestamp", lag("timestamp", 1).over(window_spec)) \
            .withColumn("prev_speed", lag("speed", 1).over(window_spec))
        
        # Calculate distance traveled
        derived_df = derived_df.withColumn(
            "distance_traveled",
            sqrt(
                pow(col("x_coord") - col("prev_x"), 2) +
                pow(col("y_coord") - col("prev_y"), 2)
            )
        )
        
        # Calculate time delta
        derived_df = derived_df.withColumn(
            "time_delta",
            unix_timestamp("timestamp") - unix_timestamp("prev_timestamp")
        )
        
        # Recalculate speed if missing
        derived_df = derived_df.withColumn(
            "calculated_speed",
            when(
                col("time_delta") > 0,
                col("distance_traveled") / col("time_delta")
            ).otherwise(0)
        )
        
        # Use provided speed or calculated speed
        derived_df = derived_df.withColumn(
            "final_speed",
            when(col("speed").isNull(), col("calculated_speed")).otherwise(col("speed"))
        )
        
        # Calculate acceleration
        derived_df = derived_df.withColumn(
            "calculated_acceleration",
            when(
                col("time_delta") > 0,
                (col("final_speed") - col("prev_speed")) / col("time_delta")
            ).otherwise(0)
        )
        
        return derived_df
    
    def aggregate_by_play(self, df):
        """Aggregate tracking metrics by play for downstream analysis."""
        # Aggregate key metrics per play per player
        play_aggregates = df \
            .groupBy("game_id", "play_id", "player_id", "team", "position") \
            .agg(
                avg("final_speed").alias("avg_speed"),
                sqrt(avg(pow(col("final_speed") - avg("final_speed"), 2))).alias("speed_variance"),
                avg("calculated_acceleration").alias("avg_acceleration"),
                avg("distance_traveled").alias("total_distance")
            )
        
        return play_aggregates
    
    def write_to_bigquery(self, df, table_name, output_mode="append"):
        """Write streaming data to BigQuery."""
        query = df \
            .writeStream \
            .format("bigquery") \
            .option("table", f"{self.project_id}:{self.bq_dataset}.{table_name}") \
            .option("checkpointLocation", f"{self.checkpoint_location}/{table_name}") \
            .option("failOnDataLoss", "false") \
            .option("writeMethod", "direct") \
            .outputMode(output_mode) \
            .trigger(processingTime="2 seconds")
        
        return query
    
    def run(self):
        """Run the tracking data streaming pipeline."""
        try:
            logger.info("Starting Player Tracking streaming processor...")
            
            # Read from Pub/Sub
            raw_stream = self.read_from_pubsub()
            
            # Parse and validate
            parsed_stream = self.parse_and_validate(raw_stream)
            
            # Calculate derived metrics
            enriched_stream = self.calculate_derived_metrics(parsed_stream)
            
            # Filter valid records
            valid_tracking = enriched_stream.filter(col("is_valid") == True)
            invalid_tracking = enriched_stream.filter(col("is_valid") == False)
            
            # Write raw tracking data to bronze layer
            valid_query = self.write_to_bigquery(
                valid_tracking.select(
                    "game_id", "play_id", "timestamp", "frame_id",
                    "player_id", "team", "jersey_number", "position",
                    "x_coord", "y_coord", "final_speed", "calculated_acceleration",
                    "direction", "orientation", "event", "distance_traveled",
                    "processing_timestamp"
                ),
                "bronze_tracking"
            ).start()
            
            # Write invalid records to DLQ
            invalid_query = self.write_to_bigquery(
                invalid_tracking,
                "bronze_tracking_dlq"
            ).start()
            
            logger.info("Player tracking streaming queries started successfully")
            
            # Wait for termination
            valid_query.awaitTermination()
            
        except Exception as e:
            logger.error(f"Error in player tracking pipeline: {str(e)}")
            raise
        finally:
            self.spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: player_tracking_processor.py <project_id> <pubsub_subscription> <bq_dataset> <checkpoint_location>")
        sys.exit(1)
    
    project_id = sys.argv[1]
    pubsub_subscription = sys.argv[2]
    bq_dataset = sys.argv[3]
    checkpoint_location = sys.argv[4]
    
    processor = PlayerTrackingProcessor(
        project_id=project_id,
        pubsub_subscription=pubsub_subscription,
        bq_dataset=bq_dataset,
        checkpoint_location=checkpoint_location
    )
    
    processor.run()
