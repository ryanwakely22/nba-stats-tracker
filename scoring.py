import pandas as pd
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='scoring.log'
)

def calculate_custom_score(player_stats):
    """
    Calculate custom score based on various stats using custom formula with specific weights.
    """

    try:
        player_stats = player_stats.fillna(0)
        
        # Convert MIN (which is typically in format like '24:30') to numerical minutes
        player_stats['MIN_NUMERIC'] = player_stats['MIN'].apply(lambda x: 
            sum(float(x) * 60**i for i, x in enumerate(reversed(x.split(':')))) / 60 
            if isinstance(x, str) and ':' in x else float(x))
        
        player_stats['CUSTOM_SCORE'] = (
            player_stats['PTS'] * 6.3755 +           # Points
            player_stats['REB'] * 0.7251 +           # Rebounds
            player_stats['AST'] * 2.6642 +           # Assists
            player_stats['STL'] * 12.9899 +          # Steals
            player_stats['BLK'] * 3.1950 -           # Blocks
            player_stats['TO'] * 8.1388 -            # Turnovers (negative impact)
            player_stats['FGA'] * 5.6948 -           # Field goals attempted
            player_stats['FTA'] * 1.9597 -           # Free throws attempted
            player_stats['MIN_NUMERIC'] * 1.233      # Minutes played (negative impact)
        )
        
        # Round to 2 decimal places
        player_stats['CUSTOM_SCORE'] = player_stats['CUSTOM_SCORE'].round(2)
        logging.info("Successfully calculated custom scores")
        
        return player_stats
    
    except Exception as e:
        logging.error(f"Error calculating custom score: {str(e)}")
        # Return original DataFrame if calculation fails
        return player_stats

def get_top_scorers(player_stats, limit=20):
    """
    Return the top N players based on custom score
    """
    try:
        if player_stats.empty:
            logging.warning("Empty player stats dataframe provided")
            return pd.DataFrame()
            
        # Calculate custom score
        scored_players = calculate_custom_score(player_stats)
        scored_players = scored_players[scored_players['MIN_NUMERIC'] > 0]

        
        # Sort by custom score (descending)
        top_players = scored_players.sort_values('CUSTOM_SCORE', ascending=False).head(limit)
        bottom_players = scored_players.sort_values('CUSTOM_SCORE', ascending=True).head(10)

        
        # Select relevant columns
        result = top_players[[
            'PLAYER_NAME', 'TEAM_ABBREVIATION', 'MIN', 'PTS', 'REB', 'AST', 
            'STL', 'BLK', 'TO', 'FGA', 'FTA', 'CUSTOM_SCORE'
        ]]
        
        logging.info(f"Successfully filtered top {len(result)} players")
        return top_players,bottom_players
        
    except Exception as e:
        logging.error(f"Error getting top scorers: {str(e)}")
        return pd.DataFrame()
