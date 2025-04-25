import sqlite3
import pandas as pd
from datetime import datetime
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='database.log'
)

DB_NAME = 'nba_scores.db'

def init_db():
    """Initialize the database with required tables"""
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        # Create players table with all needed columns
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS top_scorers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_name TEXT,
            team TEXT,
            minutes TEXT,
            points INTEGER,
            rebounds INTEGER,
            assists INTEGER,
            steals INTEGER,
            blocks INTEGER,
            turnovers INTEGER,
            field_goal_attempts INTEGER,
            free_throw_attempts INTEGER,
            custom_score REAL,
            timestamp DATETIME
        )
        ''')
        
        # Create a table to track updates
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS updates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            update_time DATETIME,
            games_processed INTEGER
        )
        ''')
        
        conn.commit()
        conn.close()
        logging.info("Database initialized successfully")
        return True
        
    except Exception as e:
        logging.error(f"Error initializing database: {str(e)}")
        return False

def save_top_scorers(top_scorers_df):
    """Save top scorers to database"""
    try:
        if top_scorers_df.empty:
            logging.warning("Attempted to save empty dataframe")
            return False
            
        conn = sqlite3.connect(DB_NAME)
        
        # First, clear existing data to avoid duplicates
        cursor = conn.cursor()
        cursor.execute("DELETE FROM top_scorers")
        conn.commit()
        
        # Convert DataFrame to database records
        records = []
        for _, row in top_scorers_df.iterrows():
            records.append((
                row['PLAYER_NAME'],
                row['TEAM_ABBREVIATION'],
                row['MIN'],
                int(row['PTS']),
                int(row['REB']),
                int(row['AST']),
                int(row['STL']),
                int(row['BLK']),
                int(row['TO']),
                int(row['FGA']),
                int(row['FTA']),
                float(row['CUSTOM_SCORE']),
                datetime.now()
            ))
        
        # Insert records
        cursor.executemany('''
        INSERT INTO top_scorers (
            player_name, team, minutes, points, rebounds, assists, 
            steals, blocks, turnovers, field_goal_attempts, free_throw_attempts,
            custom_score, timestamp
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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

def get_latest_scorers():
    """Retrieve the latest top scorers from database"""
    try:
        if not os.path.exists(DB_NAME):
            logging.warning(f"Database file {DB_NAME} does not exist")
            return pd.DataFrame()
            
        conn = sqlite3.connect(DB_NAME)
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
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(update_time) FROM updates")
        last_update = cursor.fetchone()[0]
        conn.close()
        
        return last_update
        
    except Exception as e:
        logging.error(f"Error getting last update time: {str(e)}")
        return None