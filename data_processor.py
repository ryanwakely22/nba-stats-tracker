# data_processor.py - Improved version with better error handling

from nba_data import get_games_last_12_hours, get_player_stats, get_live_games, get_live_player_stats
from scoring import calculate_custom_score, get_top_scorers
from database import init_db, save_top_scorers, save_live_data, clear_live_data
import logging
import time
import sys

# Configure logging to console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
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
            return pd.DataFrame()  # Return empty DataFrame instead of None
        
        logging.info(f"Found {len(game_ids)} completed games. Fetching player stats...")
        
        # Get player stats for these games
        player_stats = get_player_stats(game_ids)
        
        if player_stats.empty:
            logging.info("No player stats retrieved.")
            return pd.DataFrame()
        
        logging.info(f"Retrieved stats for {len(player_stats)} players")
        
        # Validate required columns
        required_columns = ['PLAYER_NAME', 'TEAM_ABBREVIATION', 'MIN', 'PTS', 'OREB', 'DREB', 
                          'AST', 'STL', 'BLK', 'TO', 'FGM', 'FGA', 'FG3M', 'FG3A', 'PF']
        missing_columns = [col for col in required_columns if col not in player_stats.columns]
        
        if missing_columns:
            logging.error(f"Missing required columns: {missing_columns}")
            return pd.DataFrame()
        
        # Calculate top scorers
        logging.info("Calculating custom scores...")
        try:
            top_players = get_top_scorers(player_stats, limit=100)
        except Exception as e:
            logging.error(f"Error calculating custom scores: {str(e)}")
            return pd.DataFrame()
        
        if top_players.empty:
            logging.warning("No players with valid stats found")
            return pd.DataFrame()
        
        logging.info(f"Calculated custom scores for {len(top_players)} players")
        
        # Save to database
        logging.info("Saving top scorers to database...")
        try:
            save_result = save_top_scorers(top_players)
        except Exception as e:
            logging.error(f"Error saving to database: {str(e)}")
            return pd.DataFrame()
        
        if save_result:
            logging.info(f"Successfully saved {len(top_players)} player records to database")
        else:
            logging.error("Failed to save player records to database")
            return pd.DataFrame()
        
        elapsed_time = time.time() - start_time
        logging.info(f"Update completed successfully in {elapsed_time:.2f} seconds!")
        return top_players
        
    except Exception as e:
        logging.error(f"Error updating top scorers: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return pd.DataFrame()

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
        
        logging.info(f"Retrieved stats for {len(player_stats)} players from live games")
        
        # Log the data structure
        logging.info(f"Live data columns: {player_stats.columns.tolist()}")
        if not player_stats.empty:
            sample_player = player_stats.iloc[0].to_dict()
            logging.info(f"Sample player data: {sample_player}")
        
        # Calculate custom scores - make sure is_live=True so we include all players
        live_player_data = get_top_scorers(player_stats, limit=100, is_live=True)
        
        if live_player_data.empty:
            logging.warning("No valid player data after processing")
            clear_live_data()
            return None
        
        logging.info(f"Processed {len(live_player_data)} players with custom scores")
        
        # Save to database (live data table)
        save_result = save_live_data(live_player_data)
        
        if save_result:
            logging.info(f"Successfully saved {len(live_player_data)} live player records to database")
        else:
            logging.error("Failed to save live player records to database")
            return None
        
        logging.info("Live game update completed successfully!")
        return live_player_data
        
    except Exception as e:
        logging.error(f"Error updating live games: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return None
