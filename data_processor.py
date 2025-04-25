from nba_data import get_games_for_display, get_player_stats
from scoring import calculate_custom_score, get_top_scorers
from database import init_db, save_top_scorers
import logging
import time

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='nba_processor.log'
)

def update_top_scorers():
    """Update the database with latest top scorers"""
    try:
        start_time = time.time()
        logging.info("Starting top scorers update...")
        
        # Get recent games
        logging.info("Fetching games from the last 12 hours...")
        game_ids = get_games_for_display()
        
        if not game_ids:
            logging.info("No completed games found in the last 12 hours.")
            return None
        
        logging.info(f"Found {len(game_ids)} completed games. Fetching player stats...")
        
        # Get player stats for these games
        player_stats = get_player_stats(game_ids)
        
        if player_stats.empty:
            logging.info("No player stats retrieved.")
            return None
        
        # Calculate top scorers
        logging.info("Calculating custom scores...")
        top_players = get_top_scorers(player_stats, limit=50)
        
        if top_players.empty:
            logging.warning("No players with valid stats found")
            return None
        
        # Save to database
        logging.info("Saving top scorers to database...")
        save_top_scorers(top_players)
        
        elapsed_time = time.time() - start_time
        logging.info(f"Update completed successfully in {elapsed_time:.2f} seconds!")
        return top_players
        
    except Exception as e:
        logging.error(f"Error updating top scorers: {str(e)}")
        return None

if __name__ == "__main__":
    # Initialize database
    init_db()
    
    # Process data
    update_top_scorers()
