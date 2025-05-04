# nba_data.py - Improved version with better error handling and debugging

from nba_api.stats.endpoints import ScoreboardV2, BoxScoreTraditionalV2
from nba_api.stats.static import players
from datetime import datetime, timedelta
import pandas as pd
import logging
import sys
import time
import json
from requests.exceptions import RequestException

# Configure logging to console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# Try to patch the NBA API to use custom headers
try:
    from nba_api.library.http import NBAStatsHTTP
    
    # Store the original method
    original_send_api_request = NBAStatsHTTP.send_api_request
    
    # Create a patched version with custom headers
    def patched_send_api_request(self, *args, **kwargs):
        # Add custom headers
        if 'headers' not in kwargs:
            kwargs['headers'] = {}
        
        kwargs['headers'].update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Origin': 'https://www.nba.com',
            'Referer': 'https://www.nba.com/',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site'
        })
        
        # Add a small delay to avoid rate limiting
        time.sleep(0.5)
        
        return original_send_api_request(self, *args, **kwargs)
    
    # Apply the patch
    NBAStatsHTTP.send_api_request = patched_send_api_request
    logging.info("Successfully patched NBA API with custom headers")
except Exception as e:
    logging.warning(f"Could not patch NBA API headers: {e}")

def parse_minutes(minutes_str):
    """Parse ISO 8601 duration format from NBA API (e.g., 'PT17M14.00S')"""
    try:
        if not minutes_str or not isinstance(minutes_str, str):
            return 0.0
            
        # Strip the 'PT' prefix
        if minutes_str.startswith('PT'):
            minutes_str = minutes_str[2:]
            
        # Initialize minutes and seconds
        minutes = 0
        seconds = 0
        
        # Extract minutes
        if 'M' in minutes_str:
            min_parts = minutes_str.split('M')
            minutes = float(min_parts[0])
            minutes_str = min_parts[1] if len(min_parts) > 1 else ''
            
        # Extract seconds
        if 'S' in minutes_str:
            sec_parts = minutes_str.split('S')
            seconds = float(sec_parts[0])
            
        # Convert to decimal minutes (e.g., 17:30 becomes 17.5)
        total_minutes = minutes + (seconds / 60)
        return round(total_minutes, 2)
        
    except Exception as e:
        logging.error(f"Error parsing minutes '{minutes_str}': {str(e)}")
        return 0.01  # Default to a small non-zero value

def format_minutes(minutes_value):
    """Format minutes as MM:SS"""
    try:
        # If minutes_value is a string with a colon, clean it up
        if isinstance(minutes_value, str) and ':' in minutes_value:
            # Handle format like '43.0000000:28'
            parts = minutes_value.split(':')
            if '.' in parts[0]:
                minutes_part = parts[0].split('.')[0]  # Take just the integer part
                minutes_int = int(minutes_part)
            else:
                minutes_int = int(parts[0])
            
            seconds_int = int(parts[1]) if parts[1].isdigit() else 0
            return f"{minutes_int}:{seconds_int:02d}"
        
        # Handle numeric value
        minutes_int = int(minutes_value)
        seconds_int = int((minutes_value - minutes_int) * 60)
        return f"{minutes_int}:{seconds_int:02d}"
    except Exception as e:
        logging.error(f"Error formatting minutes {minutes_value}: {str(e)}")
        return "0:00"

def test_nba_api_connection():
    """Test if we can connect to NBA API"""
    try:
        # Try a simple API call
        today = datetime.now().strftime('%m/%d/%Y')
        logging.info(f"Testing NBA API connection with date: {today}")
        
        scoreboard = ScoreboardV2(game_date=today)
        games_df = scoreboard.game_header.get_data_frame()
        
        logging.info(f"Successfully connected to NBA API. Found {len(games_df)} games.")
        return True
    except Exception as e:
        logging.error(f"NBA API connection test failed: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return False

def get_games_last_12_hours():
    """
    Fetch games that completed within the last 12 hours
    """
    try:
        # Test API connection first
        if not test_nba_api_connection():
            logging.error("NBA API connection test failed. Aborting data fetch.")
            return []
        
        # Calculate date range for the last 12 hours
        now = datetime.now()
        yesterday = now - timedelta(hours=24)  # Look back a full day to be safe
        
        completed_games_list = []
        
        # Check both yesterday and today
        for date_to_check in [yesterday, now]:
            # For NBA API, we need the game date in MM/DD/YYYY format
            game_date = date_to_check.strftime('%m/%d/%Y')
            logging.info(f"Fetching games for {game_date}")
            
            try:
                # Add retry logic for individual API calls
                max_retries = 3
                retry_count = 0
                
                while retry_count < max_retries:
                    try:
                        # Get scoreboard data
                        scoreboard = ScoreboardV2(game_date=game_date)
                        games_df = scoreboard.game_header.get_data_frame()
                        break
                    except Exception as e:
                        retry_count += 1
                        if retry_count == max_retries:
                            raise e
                        logging.warning(f"Retry {retry_count}/{max_retries} for date {game_date}")
                        time.sleep(2 ** retry_count)  # Exponential backoff
                
                # Log all game statuses for debugging
                if not games_df.empty:
                    status_list = games_df['GAME_STATUS_TEXT'].unique().tolist()
                    logging.info(f"Game statuses found: {status_list}")
                    
                    # Filter only completed games - check for multiple possible status texts
                    completed_games = games_df[games_df['GAME_STATUS_TEXT'].isin(['Final', 'Finished', 'Complete'])]
                    
                    if not completed_games.empty:
                        completed_games_list.extend(completed_games['GAME_ID'].tolist())
                        logging.info(f"Found {len(completed_games)} completed games for {game_date}")
                else:
                    logging.info(f"No games found for {game_date}")
                    
            except Exception as e:
                logging.error(f"Error fetching games for {game_date}: {str(e)}")
                continue
        
        logging.info(f"Total completed games found: {len(completed_games_list)}")
        return completed_games_list
    
    except Exception as e:
        logging.error(f"Error in get_games_last_12_hours: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return []

def get_player_stats(game_ids):
    """
    Fetch detailed player statistics for specified games
    """
    all_player_stats = []
    
    for game_id in game_ids:
        try:
            logging.info(f"Fetching stats for game ID: {game_id}")
            
            # Add retry logic
            max_retries = 3
            retry_count = 0
            
            while retry_count < max_retries:
                try:
                    # Get box score data
                    box_score = BoxScoreTraditionalV2(game_id=game_id)
                    player_stats = box_score.player_stats.get_data_frame()
                    break
                except Exception as e:
                    retry_count += 1
                    if retry_count == max_retries:
                        raise e
                    logging.warning(f"Retry {retry_count}/{max_retries} for game {game_id}")
                    time.sleep(2 ** retry_count)
            
            # Ensure PLUS_MINUS column exists and is properly formatted
            if 'PLUS_MINUS' not in player_stats.columns:
                logging.warning(f"PLUS_MINUS column not found in API response for game {game_id}, adding default values")
                player_stats['PLUS_MINUS'] = 0
            else:
                # Convert to numeric values, handle any string values
                player_stats['PLUS_MINUS'] = pd.to_numeric(player_stats['PLUS_MINUS'], errors='coerce').fillna(0)
            
            # Add to our collection
            if not player_stats.empty:
                all_player_stats.append(player_stats)
                logging.info(f"Successfully fetched stats for {len(player_stats)} players")
            else:
                logging.warning(f"No player stats found for game {game_id}")
                
        except Exception as e:
            logging.error(f"Error fetching player stats for game {game_id}: {str(e)}")
            import traceback
            logging.error(traceback.format_exc())
            continue
    
    # Combine all stats if we have any
    if all_player_stats:
        combined_stats = pd.concat(all_player_stats)
        return combined_stats
    
    return pd.DataFrame()  # Return empty DataFrame if no games found

def get_live_games():
    """Fetch in-progress NBA games"""
    try:
        now = datetime.now()
        today_date = now.strftime('%Y-%m-%d')
        
        # Also check the previous day for games that may have started yesterday but continued into today
        yesterday = now - timedelta(days=1)
        yesterday_date = yesterday.strftime('%Y-%m-%d')
        
        logging.info(f"Checking for live games on {yesterday_date} and {today_date}")
        
        # List to store all live games
        all_live_games = []
        
        # Check games from today
        try:
            today_game_date = datetime.now().strftime('%m/%d/%Y')
            logging.info(f"Fetching games for today ({today_game_date})")
            
            today_scoreboard = ScoreboardV2(game_date=today_game_date)
            today_games_df = today_scoreboard.game_header.get_data_frame()
            
            if not today_games_df.empty:
                # Log all game statuses for debugging
                today_status_list = today_games_df['GAME_STATUS_TEXT'].unique().tolist()
                today_status_ids = today_games_df['GAME_STATUS_ID'].unique().tolist()
                logging.info(f"Today's game statuses: {today_status_list}")
                logging.info(f"Today's game status IDs: {today_status_ids}")
                
                # Get today's live games - status ID 2 indicates a live game
                today_live_games = today_games_df[today_games_df['GAME_STATUS_ID'] == 2]
                
                if not today_live_games.empty:
                    today_live_game_ids = today_live_games['GAME_ID'].tolist()
                    logging.info(f"Found {len(today_live_game_ids)} live games today with status ID 2")
                    all_live_games.extend(today_live_game_ids)
        except Exception as e:
            logging.error(f"Error fetching today's games: {str(e)}")
        
        # Check games from yesterday that might be running late
        try:
            yesterday_game_date = yesterday.strftime('%m/%d/%Y')
            logging.info(f"Fetching games for yesterday ({yesterday_game_date})")
            
            yesterday_scoreboard = ScoreboardV2(game_date=yesterday_game_date)
            yesterday_games_df = yesterday_scoreboard.game_header.get_data_frame()
            
            if not yesterday_games_df.empty:
                # Log yesterday's game statuses for debugging
                yesterday_status_list = yesterday_games_df['GAME_STATUS_TEXT'].unique().tolist()
                yesterday_status_ids = yesterday_games_df['GAME_STATUS_ID'].unique().tolist()
                logging.info(f"Yesterday's game statuses: {yesterday_status_list}")
                logging.info(f"Yesterday's game status IDs: {yesterday_status_ids}")
                
                # Get yesterday's live games
                yesterday_live_games = yesterday_games_df[yesterday_games_df['GAME_STATUS_ID'] == 2]
                
                if not yesterday_live_games.empty:
                    yesterday_live_game_ids = yesterday_live_games['GAME_ID'].tolist()
                    logging.info(f"Found {len(yesterday_live_game_ids)} live games from yesterday still in progress")
                    all_live_games.extend(yesterday_live_game_ids)
        except Exception as e:
            logging.error(f"Error fetching yesterday's games: {str(e)}")
        
        logging.info(f"Found a total of {len(all_live_games)} live games")
        return all_live_games
        
    except Exception as e:
        logging.error(f"Error in get_live_games: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return []

def get_live_player_stats(game_ids):
    """Fetch detailed player statistics for live games using live endpoints"""
    try:
        from nba_api.live.nba.endpoints import boxscore
        
        all_player_stats = []
        
        for game_id in game_ids:
            try:
                logging.info(f"Fetching live stats for game ID: {game_id}")
                
                # Add retry logic
                max_retries = 3
                retry_count = 0
                
                while retry_count < max_retries:
                    try:
                        # Use the live boxscore endpoint
                        live_box = boxscore.BoxScore(game_id=game_id)
                        live_data = live_box.get_dict()
                        break
                    except Exception as e:
                        retry_count += 1
                        if retry_count == max_retries:
                            raise e
                        logging.warning(f"Retry {retry_count}/{max_retries} for live game {game_id}")
                        time.sleep(2 ** retry_count)
                
                # Extract player stats from the live data
                if 'game' in live_data:
                    game_data = live_data['game']
                    
                    # Process home team
                    if 'homeTeam' in game_data:
                        home_team = game_data['homeTeam']
                        home_abbr = home_team.get('teamTricode', '')
                        home_players = home_team.get('players', [])
                        
                        for player in home_players:
                            if player.get('status') == 'ACTIVE':
                                stats = player.get('statistics', {})
                                
                                # Parse minutes from ISO format
                                minutes_iso = stats.get('minutes', 'PT0M')
                                minutes_value = parse_minutes(minutes_iso)
                                minutes_display = format_minutes(minutes_value)
                                
                                player_stats = {
                                    'PLAYER_NAME': f"{player.get('firstName', '')} {player.get('familyName', '')}",
                                    'TEAM_ABBREVIATION': home_abbr,
                                    'MIN': minutes_display,
                                    'MIN_NUMERIC': minutes_value,
                                    'PTS': stats.get('points', 0),
                                    'OREB': stats.get('reboundsOffensive', 0),
                                    'DREB': stats.get('reboundsDefensive', 0),
                                    'AST': stats.get('assists', 0),
                                    'STL': stats.get('steals', 0),
                                    'BLK': stats.get('blocks', 0),
                                    'TO': stats.get('turnovers', 0),
                                    'FGM': stats.get('fieldGoalsMade', 0),
                                    'FGA': stats.get('fieldGoalsAttempted', 0),
                                    'FG3M': stats.get('threePointersMade', 0),
                                    'FG3A': stats.get('threePointersAttempted', 0),
                                    'PF': stats.get('foulsPersonal', 0),
                                    'PLUS_MINUS': stats.get('plusMinusPoints', 0)
                                }
                                
                                all_player_stats.append(player_stats)
                    
                    # Process away team (same logic as home team)
                    if 'awayTeam' in game_data:
                        away_team = game_data['awayTeam']
                        away_abbr = away_team.get('teamTricode', '')
                        away_players = away_team.get('players', [])
                        
                        for player in away_players:
                            if player.get('status') == 'ACTIVE':
                                stats = player.get('statistics', {})
                                
                                minutes_iso = stats.get('minutes', 'PT0M')
                                minutes_value = parse_minutes(minutes_iso)
                                minutes_display = format_minutes(minutes_value)
                                
                                player_stats = {
                                    'PLAYER_NAME': f"{player.get('firstName', '')} {player.get('familyName', '')}",
                                    'TEAM_ABBREVIATION': away_abbr,
                                    'MIN': minutes_display,
                                    'MIN_NUMERIC': minutes_value,
                                    'PTS': stats.get('points', 0),
                                    'OREB': stats.get('reboundsOffensive', 0),
                                    'DREB': stats.get('reboundsDefensive', 0),
                                    'AST': stats.get('assists', 0),
                                    'STL': stats.get('steals', 0),
                                    'BLK': stats.get('blocks', 0),
                                    'TO': stats.get('turnovers', 0),
                                    'FGM': stats.get('fieldGoalsMade', 0),
                                    'FGA': stats.get('fieldGoalsAttempted', 0),
                                    'FG3M': stats.get('threePointersMade', 0),
                                    'FG3A': stats.get('threePointersAttempted', 0),
                                    'PF': stats.get('foulsPersonal', 0),
                                    'PLUS_MINUS': stats.get('plusMinusPoints', 0)
                                }
                                
                                all_player_stats.append(player_stats)
                                
            except Exception as e:
                logging.error(f"Error processing live game {game_id}: {str(e)}")
                import traceback
                logging.error(traceback.format_exc())
                continue
        
        if all_player_stats:
            df = pd.DataFrame(all_player_stats)
            logging.info(f"Created DataFrame with {len(df)} players. Columns: {df.columns.tolist()}")
            return df
        
        return pd.DataFrame()
        
    except Exception as e:
        logging.error(f"Error in get_live_player_stats: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return pd.DataFrame()
