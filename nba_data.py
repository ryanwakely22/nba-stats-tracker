from nba_api.stats.endpoints import ScoreboardV2, BoxScoreTraditionalV2
from nba_api.stats.static import players
from datetime import datetime, timedelta
import pandas as pd
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='nba_data.log'
)

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

def get_games_last_12_hours():
    """
    Fetch games that completed within the last 12 hours
    """
    try:
        # Calculate date range for the last 12 hours
        now = datetime.now()
        yesterday = now - timedelta(hours=24)  # Look back a full day to be safe
        
        completed_games_list = []
        
        # Check both yesterday and today
        for date_to_check in [yesterday, now]:
            # For NBA API, we need the game date in MM/DD/YYYY format
            game_date = date_to_check.strftime('%m/%d/%Y')
            logging.info(f"Fetching games for {game_date}")
            
            # Get scoreboard data
            scoreboard = ScoreboardV2(game_date=game_date)
            games_df = scoreboard.game_header.get_data_frame()
            
            # Log all game statuses for debugging
            status_list = games_df['GAME_STATUS_TEXT'].unique().tolist()
            logging.info(f"Game statuses found: {status_list}")
            
            # Filter only completed games - check for multiple possible status texts
            completed_games = games_df[games_df['GAME_STATUS_TEXT'].isin(['Final', 'Finished', 'Complete'])]
            
            if not completed_games.empty:
                completed_games_list.extend(completed_games['GAME_ID'].tolist())
                logging.info(f"Found {len(completed_games)} completed games for {game_date}")
        
        logging.info(f"Total completed games found: {len(completed_games_list)}")
        return completed_games_list
    
    except Exception as e:
        logging.error(f"Error fetching games: {str(e)}")
        return []
    
def get_player_stats(game_ids):
    """
    Fetch detailed player statistics for specified games
    """
    all_player_stats = []
    
    for game_id in game_ids:
        try:
            logging.info(f"Fetching stats for game ID: {game_id}")
            
            # Get box score data
            box_score = BoxScoreTraditionalV2(game_id=game_id)
            player_stats = box_score.player_stats.get_data_frame()
            
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
                
                # Log the full data for a couple of players to debug
                for i in range(min(3, len(player_stats))):
                    player_data = player_stats.iloc[i]
                    logging.info(f"Player {i+1}: {player_data['PLAYER_NAME']} - MIN: {player_data['MIN']} - PTS: {player_data['PTS']} - +/-: {player_data['PLUS_MINUS']}")
            else:
                logging.warning(f"No player stats found for game {game_id}")
                
        except Exception as e:
            logging.error(f"Error fetching player stats for game {game_id}: {str(e)}")
    
    # Combine all stats if we have any
    if all_player_stats:
        combined_stats = pd.concat(all_player_stats)
        return combined_stats
    
    return pd.DataFrame()  # Return empty DataFrame if no games found

def get_live_player_stats(game_ids):
    """Fetch detailed player statistics for live games using live endpoints"""
    try:
        from nba_api.live.nba.endpoints import boxscore
        
        all_player_stats = []
        
        for game_id in game_ids:
            try:
                logging.info(f"Fetching live stats for game ID: {game_id}")
                
                # Use the live boxscore endpoint
                live_box = boxscore.BoxScore(game_id=game_id)
                live_data = live_box.get_dict()
                
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
                                    'PLUS_MINUS': stats.get('plusMinusPoints', 0)  # Add plus/minus
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
                                    'PLUS_MINUS': stats.get('plusMinusPoints', 0)  # Add plus/minus
                                }
                                
                                all_player_stats.append(player_stats)
                                
            except Exception as e:
                logging.error(f"Error processing game {game_id}: {str(e)}")
        
        if all_player_stats:
            df = pd.DataFrame(all_player_stats)
            logging.info(f"Created DataFrame with {len(df)} players. Columns: {df.columns.tolist()}")
            return df
        
        return pd.DataFrame()
        
    except Exception as e:
        logging.error(f"Error in get_live_player_stats: {str(e)}")
        return pd.DataFrame()

# Update the extract_team_players function to include plus/minus
def extract_team_players(game_data, team_key):
    """Extract player data for a team from the live game data"""
    try:
        team = game_data.get(team_key, {})
        team_abbr = team.get('teamTricode', '')
        players = team.get('players', [])
        
        # Create a list to hold player data
        players_data = []
        
        for player in players:
            # Only include active players
            if player.get('status') == 'ACTIVE':
                stats = player.get('statistics', {})
                
                # Get minutes directly from the statistics object
                minutes_iso = stats.get('minutes')
                if not minutes_iso:
                    minutes_iso = 'PT0M'
                
                # Parse ISO 8601 duration format
                minutes_value = 0.0
                try:
                    if isinstance(minutes_iso, str) and minutes_iso.startswith('PT'):
                        time_str = minutes_iso[2:]  # Remove 'PT' prefix
                        
                        # Extract minutes
                        minutes = 0
                        if 'M' in time_str:
                            min_parts = time_str.split('M')
                            minutes = float(min_parts[0])
                            time_str = min_parts[1] if len(min_parts) > 1 else ''
                        
                        # Extract seconds
                        seconds = 0
                        if time_str and 'S' in time_str:
                            sec_parts = time_str.split('S')
                            seconds = float(sec_parts[0])
                        
                        minutes_value = minutes + (seconds / 60)
                        minutes_value = round(minutes_value, 2)
                except Exception as e:
                    logging.error(f"Error parsing minutes '{minutes_iso}': {str(e)}")
                    # Default to a small non-zero value
                    minutes_value = 0.01
                
                # Format for display
                minutes_int = int(minutes_value)
                seconds_int = int((minutes_value - minutes_int) * 60)
                minutes_display = f"{minutes_int}:{seconds_int:02d}"
                
                player_data = {
                    'PLAYER_NAME': f"{player.get('firstName', '')} {player.get('familyName', '')}",
                    'TEAM_ABBREVIATION': team_abbr,
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
                    'PLUS_MINUS': stats.get('plusMinusPoints', 0)  # Add plus/minus
                }
                
                players_data.append(player_data)
        
        if players_data:
            df = pd.DataFrame(players_data)
            logging.info(f"Created DataFrame with columns: {df.columns.tolist()}")
            return df
        else:
            logging.warning(f"No active players found for {team_key}")
            return None
            
    except Exception as e:
        logging.error(f"Error extracting {team_key} player data: {str(e)}")
        return None

def get_live_games():
    """Fetch in-progress NBA games with detailed stats, including checking for games that started the previous day"""
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
        logging.info(f"Fetching games for today ({today_date})")
        today_scoreboard = ScoreboardV2(game_date=today_date, day_offset=0, league_id='00')
        today_games_df = today_scoreboard.game_header.get_data_frame()
        
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
        
        # Check games from yesterday that might be running late
        logging.info(f"Fetching games for yesterday ({yesterday_date})")
        yesterday_scoreboard = ScoreboardV2(game_date=yesterday_date, day_offset=0, league_id='00')
        yesterday_games_df = yesterday_scoreboard.game_header.get_data_frame()
        
        # Log yesterday's game statuses for debugging
        yesterday_status_list = yesterday_games_df['GAME_STATUS_TEXT'].unique().tolist()
        yesterday_status_ids = yesterday_games_df['GAME_STATUS_ID'].unique().tolist()
        logging.info(f"Yesterday's game statuses: {yesterday_status_list}")
        logging.info(f"Yesterday's game status IDs: {yesterday_status_ids}")
        
        # Get yesterday's live games
        yesterday_live_games = yesterday_games_df[yesterday_games_df['GAME_STATUS_ID'] == 2]
        
        if not yesterday_live_games.empty:
            yesterday_live_game_ids = yesterday_live_games['GAME_ID'].tolist()
            logging.info(f"Found {len(yesterday_live_game_ids)} live games from yesterday still in progress with status ID 2")
            all_live_games.extend(yesterday_live_game_ids)
        
        # If no live games found from either day, check for upcoming games as a fallback
        if not all_live_games:
            logging.info("No active games found. Checking for upcoming games...")
            
            # Check today's upcoming games
            today_upcoming_games = today_games_df[today_games_df['GAME_STATUS_ID'] == 1]
            
            if not today_upcoming_games.empty:
                upcoming_game_ids = today_upcoming_games['GAME_ID'].tolist()
                logging.info(f"Found {len(upcoming_game_ids)} upcoming games with status ID 1")
                return upcoming_game_ids
            else:
                logging.info("No upcoming games found with status ID 1")
                return []
        
        logging.info(f"Found a total of {len(all_live_games)} live games across both days")
        return all_live_games
        
    except Exception as e:
        logging.error(f"Error fetching live games: {str(e)}")
        return []

def parse_iso_duration(duration):
    """Parse ISO 8601 duration format from NBA API (e.g., 'PT17M14.00S')"""
    try:
        if not duration or not isinstance(duration, str):
            return 0.0
            
        # Strip the 'PT' prefix
        if duration.startswith('PT'):
            duration = duration[2:]
            
        # Initialize minutes and seconds
        minutes = 0
        seconds = 0
        
        # Extract minutes
        if 'M' in duration:
            min_parts = duration.split('M')
            minutes = float(min_parts[0])
            duration = min_parts[1]
            
        # Extract seconds
        if 'S' in duration:
            sec_parts = duration.split('S')
            seconds = float(sec_parts[0])
            
        # Convert to decimal minutes (e.g., 17:30 becomes 17.5)
        total_minutes = minutes + (seconds / 60)
        return round(total_minutes, 2)
        
    except Exception as e:
        logging.error(f"Error parsing duration '{duration}': {str(e)}")
        return 0.0

def process_team_players(team_data, all_player_stats):
    """Process players from a team"""
    team_abbr = team_data.get('teamTricode', '')
    players = team_data.get('players', [])
    
    for player in players:
        if player.get('status') == 'ACTIVE':
            stats = player.get('statistics', {})
            
            # Get and parse minutes - ensure it has a default value
            minutes_iso = stats.get('minutes', 'PT0M')
            if not minutes_iso or minutes_iso is None:
                minutes_iso = 'PT0M'
            
            # Parse ISO format to minutes:seconds
            minutes_display = convert_iso_to_display(minutes_iso)
            
            # Create player stats dictionary
            player_stats = {
                'PLAYER_NAME': f"{player.get('firstName', '')} {player.get('familyName', '')}",
                'TEAM_ABBREVIATION': team_abbr,
                'MIN': minutes_display,  # Ensure this is never None
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
                'PLUS_MINUS': stats.get('plusMinusPoints', 0)  # Add plus/minus
            }
            
            # Log individual player data
            logging.info(f"Player: {player_stats['PLAYER_NAME']} - Minutes: {minutes_display}")
            
            all_player_stats.append(player_stats)
