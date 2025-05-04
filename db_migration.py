import sqlite3
import logging
import time

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='migration.log'
)

DB_NAME = 'nba_scores.db'

def get_db_connection(max_retries=5, retry_delay=1.0):
    """Get a database connection with retry logic for locked database"""
    retries = 0
    while retries < max_retries:
        try:
            # Use a timeout to prevent indefinite waiting on locks
            conn = sqlite3.connect(DB_NAME, timeout=30.0, check_same_thread=False)
            # Enable WAL mode for better concurrency
            conn.execute('PRAGMA journal_mode=WAL')
            return conn
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e):
                logging.warning(f"Database locked, retrying ({retries+1}/{max_retries})...")
                time.sleep(retry_delay)
                retries += 1
            else:
                raise
    
    raise sqlite3.OperationalError("Could not access database after multiple retries")

def migrate_database():
    """Add plus_minus column to existing tables"""
    try:
        logging.info("Starting database migration to add plus_minus column")
        
        # Try to get connection with retry logic
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if plus_minus column exists in top_scorers table
        cursor.execute("PRAGMA table_info(top_scorers)")
        columns = [col[1] for col in cursor.fetchall()]
        
        if 'plus_minus' not in columns:
            logging.info("Adding plus_minus column to top_scorers table")
            cursor.execute("ALTER TABLE top_scorers ADD COLUMN plus_minus INTEGER DEFAULT 0")
        else:
            logging.info("plus_minus column already exists in top_scorers table")
        
        # Check if plus_minus column exists in live_players table
        cursor.execute("PRAGMA table_info(live_players)")
        columns = [col[1] for col in cursor.fetchall()]
        
        if 'plus_minus' not in columns:
            logging.info("Adding plus_minus column to live_players table")
            cursor.execute("ALTER TABLE live_players ADD COLUMN plus_minus INTEGER DEFAULT 0")
        else:
            logging.info("plus_minus column already exists in live_players table")
        
        conn.commit()
        conn.close()
        
        logging.info("Database migration completed successfully")
        return True
        
    except Exception as e:
        logging.error(f"Error during database migration: {str(e)}")
        return False

if __name__ == "__main__":
    migrate_database()
