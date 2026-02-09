"""Feature Engineering for ML Models

Builds features for win probability and play outcome prediction models.
Creates rolling stats, interaction terms, and contextual features from game data.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lag, lead, avg, sum as spark_sum, count,
    when, greatest, least, coalesce, lit, expr
)
from pyspark.sql.window import Window
import sys
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FeatureEngineer:
    """Transform raw play data into ML-ready features."""
    
    def __init__(self, project_id, bq_dataset, lookback_days=30):
        self.project_id = project_id
        self.bq_dataset = bq_dataset
        self.lookback_days = lookback_days
        self.spark = self._create_spark_session()
    
    def _create_spark_session(self):
        """Start Spark with BigQuery support."""
        return SparkSession.builder \
            .appName("NFL-Feature-Engineering") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
    
    def load_play_data(self):
        """Grab enriched play data from the last N days."""
        logger.info(f"Loading {self.lookback_days} days of play data")
        
        query = f"""
        SELECT *
        FROM `{self.project_id}.{self.bq_dataset}.silver_plays_enriched`
        WHERE DATE(event_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL {self.lookback_days} DAY)
        ORDER BY game_id, event_timestamp
        """
        
        return self.spark.read.format("bigquery").option("query", query).load()
    
    def add_rolling_features(self, df):
        """Add rolling averages over last 5 plays for each team."""
        # Window: last 5 plays per team
        window = Window.partitionBy("possession_team").orderBy("event_timestamp").rowsBetween(-5, -1)
        
        rolling_df = df \
            .withColumn("rolling_avg_yards", avg("yards_gained").over(window)) \
            .withColumn("rolling_success_rate", avg(when(col("play_success"), 1.0).otherwise(0.0)).over(window)) \
            .withColumn("rolling_pressure", avg("defensive_pressure_rate").over(window)) \
            .withColumn("plays_since_turnover", 
                # Count plays since last turnover
                spark_sum(when(col("is_turnover"), 1).otherwise(0)).over(window)
            )
        
        return rolling_df
    
    def add_game_context_features(self, df):
        """Add features that describe the game state."""
        context_df = df \
            .withColumn(
                "time_remaining_total",
                # Total seconds left in game
                (4 - col("quarter")) * 900 + col("time_remaining")
            ) \
            .withColumn(
                "is_winning",
                when(col("score_differential") > 0, 1).otherwise(0)
            ) \
            .withColumn(
                "is_close_game",
                when(col("score_differential").between(-7, 7), 1).otherwise(0)
            ) \
            .withColumn(
                "is_clutch_time",
                # Last 5 minutes, close game
                when(
                    (col("time_remaining_total") < 300) & (col("is_close_game") == 1),
                    1
                ).otherwise(0)
            ) \
            .withColumn(
                "field_position_category",
                # Bucketing field position: own_territory, midfield, opp_territory
                when(col("yard_line") < 40, "own_territory")
                .when(col("yard_line").between(40, 60), "midfield")
                .otherwise("opp_territory")
            )
        
        return context_df
    
    def add_down_distance_features(self, df):
        """Create features around down & distance situation."""
        dd_df = df \
            .withColumn(
                "is_passing_down",
                # 2nd & long (7+) or 3rd & medium+ (5+)
                when(
                    ((col("down") == 2) & (col("yards_to_go") >= 7)) |
                    ((col("down") == 3) & (col("yards_to_go") >= 5)),
                    1
                ).otherwise(0)
            ) \
            .withColumn(
                "is_short_yardage",
                # 3rd/4th and 2 or less
                when(
                    (col("down").isin([3, 4])) & (col("yards_to_go") <= 2),
                    1
                ).otherwise(0)
            ) \
            .withColumn(
                "down_distance_ratio",
                # Higher ratio = tougher situation
                col("down") / (col("yards_to_go") + 1)  # +1 to avoid div by zero
            )
        
        return dd_df
    
    def add_tracking_derived_features(self, df):
        """Build features from player tracking data."""
        tracking_df = df \
            .withColumn(
                "speed_advantage",
                # Offense faster than defense = positive advantage
                col("offense_avg_speed") - col("defense_avg_speed")
            ) \
            .withColumn(
                "separation_quality",
                # Bucket receiver separation into quality tiers
                when(col("avg_receiver_separation") >= 3.0, "open")
                .when(col("avg_receiver_separation") >= 1.5, "contested")
                .otherwise("covered")
            ) \
            .withColumn(
                "is_high_pressure",
                when(col("defensive_pressure_rate") > 0.3, 1).otherwise(0)
            )
        
        return tracking_df
    
    def add_interaction_features(self, df):
        """Combine multiple features to capture interactions."""
        interaction_df = df \
            .withColumn(
                "redzone_pressure",
                # Red zone + high pressure = tough situation
                col("is_red_zone").cast("int") * col("is_high_pressure")
            ) \
            .withColumn(
                "clutch_pressure",
                col("is_clutch_time") * col("is_high_pressure")
            ) \
            .withColumn(
                "winning_conservative",
                # Winning + running = likely conservative play-calling
                col("is_winning") * when(col("play_type") == "run", 1).otherwise(0)
            )
        
        return interaction_df
    
    def add_opponent_features(self, df):
        """Add defensive team strength metrics."""
        # Calculate average defensive performance per team
        def_window = Window.partitionBy("defensive_team").orderBy("event_timestamp").rowsBetween(-20, -1)
        
        opp_df = df \
            .withColumn("opp_avg_yards_allowed", avg("yards_gained").over(def_window)) \
            .withColumn("opp_success_rate_allowed", avg(when(col("play_success"), 1.0).otherwise(0.0)).over(def_window)) \
            .withColumn("opp_pressure_rate", avg("defensive_pressure_rate").over(def_window))
        
        return opp_df
    
    def select_final_features(self, df):
        """Pick the subset of features we actually want for modeling."""
        feature_cols = [
            # Identifiers
            "game_id", "play_id", "event_timestamp",
            
            # Target variable (what we're predicting)
            "play_success", "yards_gained",
            
            # Basic context
            "down", "yards_to_go", "yard_line", "quarter", "time_remaining_total",
            "score_differential", "is_winning", "is_close_game",
            
            # Game situation
            "is_red_zone", "is_two_minute_drill", "is_clutch_time",
            "field_position_category", "game_situation",
            
            # Down & distance
            "is_passing_down", "is_short_yardage", "down_distance_ratio",
            
            # Rolling stats
            "rolling_avg_yards", "rolling_success_rate", "rolling_pressure",
            "plays_since_turnover",
            
            # Tracking features
            "speed_advantage", "separation_quality", "is_high_pressure",
            "defensive_pressure_rate",
            
            # Opponent strength
            "opp_avg_yards_allowed", "opp_success_rate_allowed", "opp_pressure_rate",
            
            # Interactions
            "redzone_pressure", "clutch_pressure", "winning_conservative"
        ]
        
        return df.select(*feature_cols)
    
    def write_feature_table(self, df):
        """Save engineered features to gold layer."""
        table_name = f"{self.project_id}:{self.bq_dataset}.gold_ml_features"
        logger.info(f"Writing {df.count()} feature rows to {table_name}")
        
        df.write \
            .format("bigquery") \
            .option("table", table_name) \
            .option("writeMethod", "direct") \
            .mode("overwrite") \
            .save()
        
        logger.info("Feature engineering complete")
    
    def run(self):
        """Execute the full feature engineering pipeline."""
        try:
            logger.info("Starting feature engineering job...")
            
            # Load data
            df = self.load_play_data()
            
            # Build features step by step
            df = self.add_rolling_features(df)
            df = self.add_game_context_features(df)
            df = self.add_down_distance_features(df)
            df = self.add_tracking_derived_features(df)
            df = self.add_opponent_features(df)
            df = self.add_interaction_features(df)
            
            # Select final feature set
            final_df = self.select_final_features(df)
            
            # Write to BigQuery
            self.write_feature_table(final_df)
            
            logger.info("Feature engineering job finished successfully")
            
        except Exception as e:
            logger.error(f"Feature engineering failed: {str(e)}")
            raise
        finally:
            self.spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: feature_engineering.py <project_id> <bq_dataset> <lookback_days>")
        print("Example: feature_engineering.py my-project nfl_analytics 30")
        sys.exit(1)
    
    project_id = sys.argv[1]
    bq_dataset = sys.argv[2]
    lookback_days = int(sys.argv[3])
    
    engineer = FeatureEngineer(
        project_id=project_id,
        bq_dataset=bq_dataset,
        lookback_days=lookback_days
    )
    
    engineer.run()
