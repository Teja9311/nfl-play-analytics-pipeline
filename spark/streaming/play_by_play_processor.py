""""""Real-time Play-by-Play Event Processor

Reads play events from Pub/Sub, performs data quality checks,
and writes to BigQuery bronze layer.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, current_timestamp, window,
    when, lit, coalesce, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    FloatType, TimestampType, BooleanType
)
import sys
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PlayByPlayProcessor:
    """Processes real-time play-by-play events."""
    
    def __init__(self, project_id, pubsub_subscription, bq_dataset, checkpoint_location):
        self.project_id = project_id
        self.pubsub_subscription = pubsub_subscription
        self.bq_dataset = bq_dataset
        self.checkpoint_location = checkpoint_location
        self.spark = self._create_spark_session()
    
    def _create_spark_session(self):
        """Create Spark session with required configurations."""
        return SparkSession.builder \
            .appName("NFL-PlayByPlay-Processor") \
            .config("spark.sql.streaming.schemaInference", "true") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .getOrCreate()
    
    def get_play_schema(self):
        """Define schema for play-by-play events."""
        return StructType([
            StructField("game_id", StringType(), False),
            StructField("play_id", StringType(), False),
            StructField("event_timestamp", TimestampType(), False),
            StructField("possession_team", StringType(), False),
            StructField("defensive_team", StringType(), False),
            StructField("down", IntegerType(), True),
            StructField("yards_to_go", IntegerType(), True),
            StructField("yard_line", IntegerType(), True),
            StructField("quarter", IntegerType(), False),
            StructField("time_remaining", IntegerType(), True),
            StructField("play_type", StringType(), True),  # pass, run, punt, field_goal
            StructField("play_result", StringType(), True),  # complete, incomplete, touchdown, etc
            StructField("yards_gained", IntegerType(), True),
            StructField("is_touchdown", BooleanType(), True),
            StructField("is_turnover", BooleanType(), True),
            StructField("is_penalty", BooleanType(), True),
            StructField("penalty_yards", IntegerType(), True),
            StructField("score_home", IntegerType(), True),
            StructField("score_away", IntegerType(), True),
            StructField("formation", StringType(), True),
            StructField("personnel_offense", StringType(), True),
            StructField("personnel_defense", StringType(), True)
        ])
    
    def read_from_pubsub(self):
        """Read streaming data from Pub/Sub."""
        logger.info(f"Reading from Pub/Sub subscription: {self.pubsub_subscription}")
        
        return self.spark \
            .readStream \
            .format("pubsub") \
            .option("pubsub.subscription", self.pubsub_subscription) \
            .option("pubsub.project", self.project_id) \
            .load()
    
    def parse_and_validate(self, df):
        """Parse JSON and apply data quality checks."""
        schema = self.get_play_schema()
        
        # Parse JSON from Pub/Sub message
        parsed_df = df.select(
            from_json(col("data").cast("string"), schema).alias("play")
        ).select("play.*")
        
        # Data quality checks
        validated_df = parsed_df \
            .withColumn("processing_timestamp", current_timestamp()) \
            .withColumn(
                "is_valid",
                when(
                    (col("down").between(1, 4) | col("down").isNull()) &
                    (col("quarter").between(1, 5)) &
                    (col("yard_line").between(1, 100) | col("yard_line").isNull()) &
                    (col("yards_to_go") >= 0),
                    True
                ).otherwise(False)
            ) \
            .withColumn(
                "is_red_zone",
                when(col("yard_line") <= 20, True).otherwise(False)
            ) \
            .withColumn(
                "is_two_minute_drill",
                when(col("time_remaining") <= 120, True).otherwise(False)
            )
        
        return validated_df
    
    def enrich_play_data(self, df):
        """Add derived fields for analysis."""
        enriched_df = df \
            .withColumn(
                "play_success",
                when(
                    (col("down") == 1) & (col("yards_gained") >= col("yards_to_go") * 0.4), True
                ).when(
                    (col("down") == 2) & (col("yards_gained") >= col("yards_to_go") * 0.6), True
                ).when(
                    (col("down").isin([3, 4])) & (col("yards_gained") >= col("yards_to_go")), True
                ).otherwise(False)
            ) \
            .withColumn(
                "score_differential",
                col("score_home") - col("score_away")
            ) \
            .withColumn(
                "game_situation",
                when(col("is_red_zone"), "red_zone")
                .when(col("is_two_minute_drill"), "two_minute_drill")
                .when(abs(col("score_differential")) > 21, "garbage_time")
                .otherwise("normal")
            )
        
        return enriched_df
    
    def write_to_bigquery(self, df, table_name):
        """Write streaming data to BigQuery."""
        query = df \
            .writeStream \
            .format("bigquery") \
            .option("table", f"{self.project_id}:{self.bq_dataset}.{table_name}") \
            .option("checkpointLocation", f"{self.checkpoint_location}/{table_name}") \
            .option("failOnDataLoss", "false") \
            .option("writeMethod", "direct") \
            .outputMode("append") \
            .trigger(processingTime="5 seconds")
        
        return query
    
    def run(self):
        """Run the streaming pipeline."""
        try:
            logger.info("Starting Play-by-Play streaming processor...")
            
            # Read from Pub/Sub
            raw_stream = self.read_from_pubsub()
            
            # Parse and validate
            parsed_stream = self.parse_and_validate(raw_stream)
            
            # Enrich
            enriched_stream = self.enrich_play_data(parsed_stream)
            
            # Split valid and invalid records
            valid_plays = enriched_stream.filter(col("is_valid") == True)
            invalid_plays = enriched_stream.filter(col("is_valid") == False)
            
            # Write valid plays to bronze layer
            valid_query = self.write_to_bigquery(valid_plays, "bronze_plays").start()
            
            # Write invalid plays to dead letter queue
            invalid_query = self.write_to_bigquery(invalid_plays, "bronze_plays_dlq").start()
            
            logger.info("Streaming queries started successfully")
            
            # Wait for termination
            valid_query.awaitTermination()
            
        except Exception as e:
            logger.error(f"Error in streaming pipeline: {str(e)}")
            raise
        finally:
            self.spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: play_by_play_processor.py <project_id> <pubsub_subscription> <bq_dataset> <checkpoint_location>")
        sys.exit(1)
    
    project_id = sys.argv[1]
    pubsub_subscription = sys.argv[2]
    bq_dataset = sys.argv[3]
    checkpoint_location = sys.argv[4]
    
    processor = PlayByPlayProcessor(
        project_id=project_id,
        pubsub_subscription=pubsub_subscription,
        bq_dataset=bq_dataset,
        checkpoint_location=checkpoint_location
    )
    
    processor.run()
