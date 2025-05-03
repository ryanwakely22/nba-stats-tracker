import pandas as pd
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='scoring.log'
)

def calculate_custom_score(player_stats):
    """Calculate custom score based on various stats using custom formula with specific weights."""
    try:
        # Replace all None/NaN values with 0
        player_stats = player_stats.fillna(0)
        
        # Ensure MIN_NUMERIC exists - derive from MIN if needed
        if 'MIN_NUMERIC' not in player_stats.columns and 'MIN' in player_stats.columns:
            logging.info("Creating MIN_NUMERIC from MIN column")
            # Convert MIN (which is typically in format like '24:30') to numerical minutes
            try:
                player_stats['MIN_NUMERIC'] = player_stats['MIN'].apply(lambda x: 
                    sum(float(i) * 60**idx for idx, i in enumerate(reversed(str(x).split(':')))) / 60 
                    if ':' in str(x) else float(x))
            except Exception as e:
                logging.error(f"Error creating MIN_NUMERIC: {str(e)}")
                player_stats['MIN_NUMERIC'] = 0.01  # Default value
        elif 'MIN_NUMERIC' not in player_stats.columns:
            logging.warning("No MIN column available to create MIN_NUMERIC, using default")
            player_stats['MIN_NUMERIC'] = 0.01  # Default value

        # Ensure all stat columns are numeric
        for col in ['PTS', 'OREB', 'DREB', 'AST', 'STL', 'BLK', 'TO', 'FGA', 'FGM', 'FG3M', 'FG3A', 'PF', 'MIN_NUMERIC','PLUS_MINUS']:
            if col in player_stats.columns:
                player_stats[col] = pd.to_numeric(player_stats[col], errors='coerce').fillna(0)
            else:
                logging.warning(f"Column {col} not found, using zeros")
                player_stats[col] = 0
        
        # Calculate custom score
        player_stats['CUSTOM_SCORE'] = (
            player_stats['PTS'] * 4.5546 +           
            player_stats['OREB'] * 1.1876 +          
            player_stats['DREB'] * 1.1876 +          
            player_stats['AST'] * 1.8509 +           
            player_stats['STL'] * 12.1842 +          
            player_stats['BLK'] * 4.0437 -           
            player_stats['TO'] * 3.5363 -           
            player_stats['FGA'] * 4.8825 +           
            player_stats['FGM'] * 0 +            
            player_stats['FG3M'] * 9.5992 -          
            player_stats['FG3A'] * 2.2564 -          
            player_stats['PF'] * 2.109 -            
            player_stats['MIN_NUMERIC'] * 0.7015 +
            player_stats['PLUS_MINUS'] * 0.5746
            )/10
        
        # Round to 2 decimal places
        player_stats['CUSTOM_SCORE'] = player_stats['CUSTOM_SCORE'].round(2)
        logging.info("Successfully calculated custom scores")
        
        return player_stats
    
    except Exception as e:
        logging.error(f"Error calculating custom score: {str(e)}")
        # Log full exception details for debugging
        import traceback
        logging.error(traceback.format_exc())
        return player_stats

def get_top_scorers(player_stats, limit=200, is_live=False):
    """Return the top N players based on custom score"""
    try:
        if player_stats.empty:
            logging.warning("Empty player stats dataframe provided")
            return pd.DataFrame()
        
        # Log what we have before processing
        logging.info(f"get_top_scorers received dataframe with columns: {player_stats.columns.tolist()}")
        
        # Create MIN_NUMERIC from MIN if needed for scoring but don't replace MIN
        if 'MIN_NUMERIC' not in player_stats.columns:
            # Create a copy of the dataframe to avoid modifying the original
            scored_players = player_stats.copy()
            
            # Convert MIN to a numeric value for scoring purposes only
            def min_to_numeric(min_str):
                try:
                    if min_str is None:
                        return 0.0
                    min_str = str(min_str)  # Ensure it's a string
                    if ':' in min_str:
                        parts = min_str.split(':')
                        # Handle case where the minutes part contains a decimal
                        if '.' in parts[0]:
                            minutes = float(parts[0])
                        else:
                            minutes = int(parts[0])
                        seconds = int(parts[1]) if parts[1].isdigit() else 0
                        return minutes + (seconds / 60)
                    return float(min_str) if min_str else 0.0
                except Exception as e:
                    logging.error(f"Error converting '{min_str}' to numeric: {str(e)}")
                    return 0.0

            scored_players['MIN_NUMERIC'] = scored_players['MIN'].apply(min_to_numeric)
        else:
            scored_players = player_stats
        
        # Calculate custom score
        scored_players = calculate_custom_score(scored_players)
        
        # Filter out players with 0 minutes regardless of whether it's live or completed games
        filtered_players = scored_players[scored_players['MIN_NUMERIC'] > 0]
        logging.info(f"Filtered to {len(filtered_players)} players with minutes > 0")
        
        # Sort by custom score (descending)
        top_players = filtered_players.sort_values('CUSTOM_SCORE', ascending=False).head(limit)
        
        # Ensure all needed columns are present
        needed_columns = [
            'PLAYER_NAME', 'TEAM_ABBREVIATION', 'MIN', 'PTS', 'OREB', 'DREB',
            'AST', 'STL', 'BLK', 'TO', 'FGM', 'FGA', 'FG3M', 'FG3A', 'PF','PLUS_MINUS','CUSTOM_SCORE'
        ]
        
        # Only include columns that exist
        columns_to_select = [col for col in needed_columns if col in top_players.columns]
        result = top_players[columns_to_select]
        
        logging.info(f"Returning {len(result)} players from get_top_scorers")
        return result
        
    except Exception as e:
        logging.error(f"Error getting top scorers: {str(e)}")
        return pd.DataFrame()
