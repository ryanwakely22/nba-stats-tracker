from nba_data import get_games_last_12_hours, get_player_stats, get_live_games, get_live_player_stats
from scoring import calculate_custom_score, get_top_scorers
from database import init_db, save_top_scorers, save_live_data, clear_live_data
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
        game_ids = get_games_last_12_hours()
        
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
        top_players = get_top_scorers(player_stats, limit=100)
        
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

def update_live_games():
    """Update the database with latest live game stats"""
    try:
        logging.info("Starting live games update...")
        
        # Get in-progress games only
        live_games = get_live_games()
        
        if not live_games:
            logging.info("No live games found. Clearing live data...")
            # Clear the live data table since no games are currently active
            clear_live_data()
            return None
        
        logging.info(f"Found {len(live_games)} live games. Fetching player stats...")
        
        # Get player stats for live games using the live endpoint
        player_stats = get_live_player_stats(live_games)
        
        if player_stats.empty:
            logging.info("No player stats retrieved for live games. Clearing live data...")
            # Clear the live data table if no valid stats
            clear_live_data()
            return None
        
        # Log the data structure
        logging.info(f"Live data columns: {player_stats.columns.tolist()}")
        if not player_stats.empty:
            logging.info(f"Sample player: {player_stats.iloc[0].to_dict()}")
        
        # Calculate custom scores - make sure is_live=True so we include all players
        live_player_data = get_top_scorers(player_stats, limit=100, is_live=True)
        
        # Save to database (live data table)
        save_result = save_live_data(live_player_data)
        logging.info(f"Save result: {save_result}")
        
        logging.info("Live game update completed successfully!")
        return live_player_data
        
    except Exception as e:
        logging.error(f"Error updating live games: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return None
