import sqlite3
import pandas as pd
from datetime import datetime
import logging
import os
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='database.log'
)

DB_NAME = 'nba_scores.db'

def get_db_connection(max_retries=5, retry_delay=1.0):
    """Get a database connection with retry logic for locked database"""
    retries = 0
    while retries < max_retries:
        try:
            # Use a timeout to prevent indefinite waiting on locks
            conn = sqlite3.connect(DB_NAME, timeout=20.0)
            return conn
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e):
                logging.warning(f"Database locked, retrying ({retries+1}/{max_retries})...")
                time.sleep(retry_delay)
                retries += 1
            else:
                # Re-raise if it's not a locking error
                raise
    
    # If we get here, we've exhausted our retries
    raise sqlite3.OperationalError("Could not access database after multiple retries - database is locked")

def init_db():
    """Initialize the database with required tables"""
    try:
        conn = sqlite3.connect(DB_NAME)
        
        with conn:
            # Create top_scorers table with all needed columns
            conn.execute('''
            CREATE TABLE IF NOT EXISTS top_scorers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_name TEXT,
                team TEXT,
                minutes TEXT,
                min_numeric REAL,
                points INTEGER,
                offensive_rebounds INTEGER,
                defensive_rebounds INTEGER,
                assists INTEGER,
                steals INTEGER,
                blocks INTEGER,
                turnovers INTEGER,
                field_goal_made INTEGER,
                field_goal_attempts INTEGER,
                three_point_made INTEGER,
                three_point_attempts INTEGER,
                personal_fouls INTEGER,
                free_throw_attempts INTEGER,
                plus_minus INTEGER,
                custom_score REAL,
                timestamp DATETIME
            )
            ''')
            
            # Create live_players table with all needed columns
            conn.execute('''
            CREATE TABLE IF NOT EXISTS live_players (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_name TEXT,
                team TEXT,
                minutes TEXT,
                min_numeric REAL,
                points INTEGER,
                offensive_rebounds INTEGER,
                defensive_rebounds INTEGER,
                assists INTEGER,
                steals INTEGER,
                blocks INTEGER,
                turnovers INTEGER,
                field_goal_made INTEGER,
                field_goal_attempts INTEGER,
                three_point_made INTEGER,
                three_point_attempts INTEGER,
                personal_fouls INTEGER,
                plus_minus INTEGER,
                custom_score REAL,
                timestamp DATETIME
            )
            ''')
            
            # Create a table to track updates
            conn.execute('''
            CREATE TABLE IF NOT EXISTS updates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                update_time DATETIME,
                games_processed INTEGER
            )
            ''')
        
        logging.info("Database initialized successfully")
        return True
        
    except Exception as e:
        logging.error(f"Error initializing database: {str(e)}")
        return False

def clear_live_data():
    """Clear all live player data when no games are active"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Clear the live_players table
        cursor.execute("DELETE FROM live_players")
        
        conn.commit()
        conn.close()
        
        logging.info("Cleared live game data from database")
        return True
        
    except Exception as e:
        logging.error(f"Error clearing live data: {str(e)}")
        return False

def save_top_scorers(top_scorers_df):
    """Save top scorers to database"""
    try:
        if top_scorers_df.empty:
            logging.warning("Attempted to save empty dataframe")
            return False
            
        conn = get_db_connection()
        
        # First, clear existing data to avoid duplicates
        cursor = conn.cursor()
        cursor.execute("DELETE FROM top_scorers")
        conn.commit()
        
        # Convert DataFrame to database records
        records = []
        for _, row in top_scorers_df.iterrows():
            # Get plus_minus value - handle both uppercase and lowercase column names
            plus_minus_value = 0
            if 'PLUS_MINUS' in row:
                plus_minus_value = int(row['PLUS_MINUS'])
            elif 'plus_minus' in row:
                plus_minus_value = int(row['plus_minus'])
                
            records.append((
                row['PLAYER_NAME'],
                row['TEAM_ABBREVIATION'],
                row['MIN'],
                int(row['PTS']),
                int(row['OREB']),
                int(row['DREB']),
                int(row['AST']),
                int(row['STL']),
                int(row['BLK']),
                int(row['TO']),
                int(row['FGM']),
                int(row['FGA']),
                int(row['FG3M']),
                int(row['FG3A']),
                int(row['PF']),
                int(row.get('FTA', 0)),  # Use get with default in case FTA is no longer included
                plus_minus_value,  # Use the extracted plus_minus value
                float(row['CUSTOM_SCORE']),
                datetime.now()
            ))
        
        # Insert records with existing columns only
        cursor.executemany('''
        INSERT INTO top_scorers (
            player_name, team, minutes, points, offensive_rebounds, defensive_rebounds, assists, 
            steals, blocks, turnovers, field_goal_made, field_goal_attempts, 
            three_point_made, three_point_attempts, personal_fouls, free_throw_attempts,
            plus_minus, custom_score, timestamp
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', records)
        
        # Log update
        cursor.execute('''
        INSERT INTO updates (update_time, games_processed)
        VALUES (?, ?)
        ''', (datetime.now(), len(records)))
        
        conn.commit()
        conn.close()
        
        logging.info(f"Saved {len(records)} player records to database")
        return True
        
    except Exception as e:
        logging.error(f"Error saving top scorers: {str(e)}")
        return False

def save_live_data(live_player_data):
    """Save live player data to database"""
    try:
        if live_player_data.empty:
            logging.warning("Attempted to save empty live data")
            return False
        
        # Log sample data for debugging
        if not live_player_data.empty:
            sample_row = live_player_data.iloc[0].to_dict()
            logging.info(f"Sample data before saving: {sample_row}")
        
        # Connect to the database
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Clear existing live data
        cursor.execute("DELETE FROM live_players")
        
        # Convert DataFrame to records
        records = []
        for _, row in live_player_data.iterrows():
            # Ensure we're using MIN, not MIN_NUMERIC for display
            minutes_display = row['MIN'] if 'MIN' in row else '0:00'
            
            # Get plus_minus value - handle both uppercase and lowercase column names
            plus_minus_value = 0
            if 'PLUS_MINUS' in row:
                plus_minus_value = int(row['PLUS_MINUS']) 
            elif 'plus_minus' in row:
                plus_minus_value = int(row['plus_minus'])
            
            record = (
                row['PLAYER_NAME'],
                row['TEAM_ABBREVIATION'],
                minutes_display,  # Use formatted minutes string
                int(row['PTS']),
                int(row['OREB']),
                int(row['DREB']),
                int(row['AST']),
                int(row['STL']),
                int(row['BLK']),
                int(row['TO']),
                int(row['FGM']),
                int(row['FGA']),
                int(row['FG3M']),
                int(row['FG3A']),
                int(row['PF']),
                plus_minus_value,  # Use the extracted plus_minus value
                float(row['CUSTOM_SCORE']),
                datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            )
            records.append(record)
        
        # Insert records
        cursor.executemany('''
        INSERT INTO live_players (
            player_name, team, minutes, points, offensive_rebounds, defensive_rebounds, assists, 
            steals, blocks, turnovers, field_goal_made, field_goal_attempts, 
            three_point_made, three_point_attempts, personal_fouls, plus_minus,
            custom_score, timestamp
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', records)
        
        # Commit changes
        conn.commit()
        conn.close()
        
        logging.info(f"Saved {len(records)} live player records to database")
        return True
        
    except Exception as e:
        logging.error(f"Error saving live player data: {str(e)}")
        return False

def get_latest_live_data():
    """Retrieve the latest live player data from database"""
    try:
        if not os.path.exists(DB_NAME):
            logging.warning(f"Database file {DB_NAME} does not exist")
            return pd.DataFrame()
            
        conn = get_db_connection()
        query = "SELECT * FROM live_players ORDER BY custom_score DESC"
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        logging.info(f"Retrieved {len(df)} live player records from database")
        return df
        
    except Exception as e:
        logging.error(f"Error retrieving live player data: {str(e)}")
        return pd.DataFrame()
    
def get_latest_scorers():
    """Retrieve the latest top scorers from database"""
    try:
        if not os.path.exists(DB_NAME):
            logging.warning(f"Database file {DB_NAME} does not exist")
            return pd.DataFrame()
            
        conn = get_db_connection()
        query = "SELECT * FROM top_scorers ORDER BY custom_score DESC"
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        logging.info(f"Retrieved {len(df)} records from database")
        return df
        
    except Exception as e:
        logging.error(f"Error retrieving top scorers: {str(e)}")
        return pd.DataFrame()

def get_last_update_time():
    """Get the timestamp of the last database update"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(update_time) FROM updates")
        last_update = cursor.fetchone()[0]
        conn.close()
        
        return last_update
        
    except Exception as e:
        logging.error(f"Error getting last update time: {str(e)}")
        return None
