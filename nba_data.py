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

def get_games_for_display():
    """
    Fetch NBA games from today (after 7 PM) or yesterday (before 7 PM),
    including both Final and In Progress games.
    """
    try:
        now = datetime.now()
        target_hour = 19  # 7 PM
        use_yesterday = now.hour < target_hour

        target_date = now - timedelta(days=1) if use_yesterday else now
        game_date_str = target_date.strftime('%m/%d/%Y')

        logging.info(f"Fetching games for display from: {game_date_str}")

        scoreboard = ScoreboardV2(game_date=game_date_str)
        games_df = scoreboard.game_header.get_data_frame()
        games_df['GAME_DATE_EST'] = pd.to_datetime(games_df['GAME_DATE_EST'])

        # Allow both completed and in-progress games
        filtered_games = games_df[
            games_df['GAME_STATUS_TEXT'].isin(['Final', 'In Progress'])
        ]

        logging.info(f"Found {len(filtered_games)} games for {game_date_str}")
        return filtered_games['GAME_ID'].tolist()

    except Exception as e:
        logging.error(f"Error fetching display games: {str(e)}")
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
            
            # Add to our collection
            if not player_stats.empty:
                all_player_stats.append(player_stats)
                logging.info(f"Successfully fetched stats for {len(player_stats)} players")
            else:
                logging.warning(f"No player stats found for game {game_id}")
                
        except Exception as e:
            logging.error(f"Error fetching player stats for game {game_id}: {str(e)}")
    
    # Combine all stats if we have any
    if all_player_stats:
        combined_stats = pd.concat(all_player_stats)
        return combined_stats
    
    return pd.DataFrame()  # Return empty DataFrame if no games found
