"""Play-by-Play Data Simulator

Generates realistic NFL play data and pushes it to Pub/Sub for testing.
Useful for dev/staging environments where you don't have real game feeds.
"""

import json
import random
import time
import uuid
from datetime import datetime, timedelta
from google.cloud import pubsub_v1
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# NFL teams for realistic sim
TEAMS = [
    "KC", "BUF", "BAL", "CIN", "SF", "PHI", "DAL", "DET",
    "GB", "MIN", "LAR", "SEA", "TB", "NO", "ATL", "CAR"
]


class PlayDataSimulator:
    """Generates and publishes fake NFL play events."""
    
    def __init__(self, project_id, topic_name, events_per_sec=2):
        self.project_id = project_id
        self.topic_name = topic_name
        self.events_per_sec = events_per_sec
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(project_id, topic_name)
        
        # Game state tracking
        self.game_id = f"game_{uuid.uuid4().hex[:8]}"
        self.current_quarter = 1
        self.time_remaining = 900  # 15 minutes
        self.down = 1
        self.yards_to_go = 10
        self.yard_line = 25  # start at own 25
        self.possession = random.choice(TEAMS)
        self.defense = random.choice([t for t in TEAMS if t != self.possession])
        self.score_home = 0
        self.score_away = 0
        self.play_count = 0
    
    def generate_play_event(self):
        """Create a single play with somewhat realistic outcomes."""
        self.play_count += 1
        
        # Pick play type based on down/distance
        if self.down <= 2 and self.yards_to_go <= 3:
            play_type = random.choices(["run", "pass"], weights=[0.6, 0.4])[0]
        elif self.down >= 3 and self.yards_to_go > 7:
            play_type = random.choices(["run", "pass"], weights=[0.2, 0.8])[0]
        else:
            play_type = random.choice(["run", "pass"])
        
        # Simulate yards gained (biased toward realistic NFL averages)
        if play_type == "run":
            yards_gained = int(random.gauss(4.5, 3.5))  # avg ~4.5 yds/carry
        else:
            yards_gained = int(random.gauss(7.0, 6.0))  # avg ~7 yds/pass
        
        yards_gained = max(-15, min(yards_gained, 80))  # clamp to sane range
        
        # Random events
        is_touchdown = (self.yard_line + yards_gained >= 100) and (yards_gained > 0)
        is_turnover = random.random() < 0.03  # ~3% turnover rate
        is_penalty = random.random() < 0.05  # ~5% penalty rate
        penalty_yards = random.choice([-5, -10, -15]) if is_penalty else 0
        
        play = {
            "game_id": self.game_id,
            "play_id": f"play_{self.play_count}",
            "event_timestamp": datetime.utcnow().isoformat() + "Z",
            "possession_team": self.possession,
            "defensive_team": self.defense,
            "down": self.down,
            "yards_to_go": self.yards_to_go,
            "yard_line": self.yard_line,
            "quarter": self.current_quarter,
            "time_remaining": self.time_remaining,
            "play_type": play_type,
            "play_result": "complete" if yards_gained > 0 else "incomplete",
            "yards_gained": yards_gained,
            "is_touchdown": is_touchdown,
            "is_turnover": is_turnover,
            "is_penalty": is_penalty,
            "penalty_yards": penalty_yards,
            "score_home": self.score_home,
            "score_away": self.score_away,
            "formation": random.choice(["shotgun", "under_center", "pistol"]),
            "personnel_offense": random.choice(["11_personnel", "12_personnel", "21_personnel"]),
            "personnel_defense": random.choice(["4-3", "3-4", "nickel", "dime"])
        }
        
        # Update game state
        self._update_game_state(yards_gained, is_touchdown, is_turnover)
        
        return play
    
    def _update_game_state(self, yards_gained, is_touchdown, is_turnover):
        """Move the game forward based on play outcome."""
        if is_touchdown:
            # Score and kickoff
            self.score_home += 7
            self.down = 1
            self.yards_to_go = 10
            self.yard_line = 25
            # Swap possession
            self.possession, self.defense = self.defense, self.possession
        elif is_turnover:
            # Swap possession
            self.possession, self.defense = self.defense, self.possession
            self.down = 1
            self.yards_to_go = 10
            self.yard_line = 50  # assume turnover at midfield
        else:
            # Normal play
            self.yard_line += yards_gained
            self.yard_line = max(1, min(self.yard_line, 99))  # keep on field
            
            if yards_gained >= self.yards_to_go:
                # First down!
                self.down = 1
                self.yards_to_go = 10
            else:
                # Didn't convert
                self.down += 1
                self.yards_to_go -= yards_gained
                
                if self.down > 4:
                    # Turnover on downs
                    self.possession, self.defense = self.defense, self.possession
                    self.down = 1
                    self.yards_to_go = 10
                    self.yard_line = 100 - self.yard_line  # flip field position
        
        # Tick clock
        self.time_remaining -= random.randint(20, 50)  # 20-50 sec per play
        if self.time_remaining <= 0:
            self.current_quarter += 1
            self.time_remaining = 900
            if self.current_quarter > 4:
                # Game over, start a new one
                logger.info(f"Game {self.game_id} finished. Final score: {self.score_home}-{self.score_away}")
                self._reset_game()
    
    def _reset_game(self):
        """Start a fresh game."""
        self.game_id = f"game_{uuid.uuid4().hex[:8]}"
        self.current_quarter = 1
        self.time_remaining = 900
        self.down = 1
        self.yards_to_go = 10
        self.yard_line = 25
        self.possession = random.choice(TEAMS)
        self.defense = random.choice([t for t in TEAMS if t != self.possession])
        self.score_home = 0
        self.score_away = 0
        self.play_count = 0
        logger.info(f"New game started: {self.game_id} - {self.possession} vs {self.defense}")
    
    def publish_play(self, play):
        """Send play event to Pub/Sub."""
        message = json.dumps(play).encode("utf-8")
        future = self.publisher.publish(self.topic_path, message)
        try:
            future.result(timeout=5)
            logger.debug(f"Published play {play['play_id']}: {play['play_type']} for {play['yards_gained']} yards")
        except Exception as e:
            logger.error(f"Failed to publish play: {e}")
    
    def run(self, duration_sec=None):
        """Start generating plays continuously."""
        logger.info(f"Starting play simulator for topic: {self.topic_name}")
        logger.info(f"Generating {self.events_per_sec} events/sec")
        
        start_time = time.time()
        
        try:
            while True:
                play = self.generate_play_event()
                self.publish_play(play)
                
                # Sleep to maintain target rate
                time.sleep(1.0 / self.events_per_sec)
                
                # Check duration if specified
                if duration_sec and (time.time() - start_time) > duration_sec:
                    logger.info(f"Duration limit reached ({duration_sec}s). Stopping.")
                    break
        
        except KeyboardInterrupt:
            logger.info("Simulator stopped by user")
        finally:
            logger.info(f"Total plays generated: {self.play_count}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Simulate NFL play-by-play events")
    parser.add_argument("--project-id", required=True, help="GCP project ID")
    parser.add_argument("--topic", required=True, help="Pub/Sub topic name")
    parser.add_argument("--rate", type=float, default=2.0, help="Events per second")
    parser.add_argument("--duration", type=int, help="Run for N seconds (optional)")
    
    args = parser.parse_args()
    
    simulator = PlayDataSimulator(
        project_id=args.project_id,
        topic_name=args.topic,
        events_per_sec=args.rate
    )
    
    simulator.run(duration_sec=args.duration)
