from flask import Flask, render_template, jsonify, request
from database import init_db, get_latest_scorers, get_last_update_time, get_latest_live_data, clear_live_data
from data_processor import update_top_scorers, update_live_games
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
import pandas as pd
import logging
import os
from datetime import datetime

# Import the migration function
from db_migration import migrate_database  # Add this import

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='app.log'
)

app = Flask(__name__)

# Initialize database
init_db()

# Perform database migration to add plus_minus column
migration_result = migrate_database()  # Add this line
if migration_result:
    logging.info("Database migration completed successfully")
else:
    logging.error("Database migration failed")

# Clear live data on startup to ensure clean state
clear_live_data()
logging.info("Cleared live data on startup")

# Create scheduler with limited concurrency to prevent database locks
executors = {
    'default': ThreadPoolExecutor(1)  # Limit to 1 thread to prevent concurrent DB access
}

# Create a scheduler for automatic updates
scheduler = BackgroundScheduler(executors=executors, job_defaults={'misfire_grace_time': 30})
scheduler.add_job(update_top_scorers, 'interval', minutes=30)

# Add live game scheduler
app.logger.info("Setting up live games scheduler")
live_scheduler = BackgroundScheduler(executors=executors, job_defaults={'misfire_grace_time': 30})
live_scheduler.add_job(update_live_games, 'interval', seconds=30)
app.logger.info("Live scheduler job created")

@app.route('/')
def index():
    """Render the main page"""
    last_update = get_last_update_time()
    if last_update:
        last_update_display = datetime.strptime(last_update, '%Y-%m-%d %H:%M:%S.%f').strftime('%Y-%m-%d %H:%M:%S')
    else:
        last_update_display = "Never"
        
    return render_template('index.html', last_update=last_update_display)

@app.route('/api/top-scorers')
def top_scorers_api():
    """API endpoint to get top scorers"""
    try:
        df = get_latest_scorers()
        
        if df.empty:
            return jsonify({'status': 'error', 'message': 'No data available', 'players': []})
        
        # Convert to list of dictionaries for JSON response
        players = []
        for _, row in df.iterrows():
            # Format minutes properly if it's in an odd format
            minutes_display = row['minutes']
            if ':' in str(minutes_display):
                try:
                    parts = str(minutes_display).split(':')
                    if '.' in parts[0]:
                        minutes_part = parts[0].split('.')[0]  # Take just the integer part
                        minutes_int = int(minutes_part)
                    else:
                        minutes_int = int(parts[0])
                    
                    seconds_int = int(parts[1]) if parts[1].isdigit() else 0
                    minutes_display = f"{minutes_int}:{seconds_int:02d}"
                except Exception as e:
                    logging.error(f"Error formatting minutes {minutes_display}: {str(e)}")

            total_rebounds = int(row['offensive_rebounds']) + int(row['defensive_rebounds'])
            
            players.append({
                'player_name': row['player_name'],
                'team': row['team'],
                'minutes': minutes_display,
                'points': int(row['points']),
                'rebounds': total_rebounds,
                'assists': int(row['assists']),
                'steals': int(row['steals']),
                'blocks': int(row['blocks']),
                'turnovers': int(row['turnovers']),
                'field_goal_made': int(row['field_goal_made']),
                'field_goal_attempts': int(row['field_goal_attempts']),
                'three_point_made': int(row['three_point_made']),
                'three_point_attempts': int(row['three_point_attempts']),
                'personal_fouls': int(row['personal_fouls']),
                'plus_minus': int(row['plus_minus']),
                'custom_score': float(row['custom_score'])
            })
        
        return jsonify({'status': 'success', 'players': players})
        
    except Exception as e:
        logging.error(f"Error in top_scorers_api: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e), 'players': []})

@app.route('/refresh')
def refresh_data():
    """Force a refresh of the data"""
    try:
        result = update_top_scorers()
        
        if result is not None:
            return jsonify({'status': 'success', 'count': len(result)})
        else:
            return jsonify({'status': 'error', 'message': 'Failed to update data'})
            
    except Exception as e:
        logging.error(f"Error in refresh_data: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/last-update')
def last_update_api():
    """API endpoint to get the last update time"""
    last_update = get_last_update_time()
    if last_update:
        return jsonify({'status': 'success', 'last_update': last_update})
    else:
        return jsonify({'status': 'error', 'message': 'No update record found'})

@app.route('/api/live-games')
def live_games_api():
    """API endpoint to get live game data"""
    try:
        df = get_latest_live_data()
        
        if df.empty:
            return jsonify({'status': 'error', 'message': 'No live games data available', 'players': []})
        
        # Convert to list of dictionaries for JSON response
        players = []
        for _, row in df.iterrows():
            # Format minutes properly if it's in an odd format
            minutes_display = row['minutes']
            if ':' in str(minutes_display):
                try:
                    parts = str(minutes_display).split(':')
                    if '.' in parts[0]:
                        minutes_part = parts[0].split('.')[0]  # Take just the integer part
                        minutes_int = int(minutes_part)
                    else:
                        minutes_int = int(parts[0])
                    
                    seconds_int = int(parts[1]) if parts[1].isdigit() else 0
                    minutes_display = f"{minutes_int}:{seconds_int:02d}"
                except Exception as e:
                    logging.error(f"Error formatting minutes {minutes_display}: {str(e)}")

            total_rebounds = int(row['offensive_rebounds']) + int(row['defensive_rebounds'])
            
            players.append({
                'player_name': row['player_name'],
                'team': row['team'],
                'minutes': minutes_display,
                'points': int(row['points']),
                'rebounds': total_rebounds,
                'assists': int(row['assists']),
                'steals': int(row['steals']),
                'blocks': int(row['blocks']),
                'turnovers': int(row['turnovers']),
                'field_goal_made': int(row['field_goal_made']),
                'field_goal_attempts': int(row['field_goal_attempts']),
                'three_point_made': int(row['three_point_made']),
                'three_point_attempts': int(row['three_point_attempts']),
                'personal_fouls': int(row['personal_fouls']),
                'plus_minus': int(row['plus_minus']),
                'custom_score': float(row['custom_score'])
            })
        
        return jsonify({'status': 'success', 'players': players})
        
    except Exception as e:
        logging.error(f"Error in live_games_api: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e), 'players': []})

@app.route('/api/live-games-status')
def live_games_status():
    """Check if there are any live games"""
    try:
        df = get_latest_live_data()
        
        has_live_games = not df.empty
        player_count = len(df) if has_live_games else 0
        
        # Try to determine unique game count (you might need to adjust this based on your data)
        unique_games = 0
        if has_live_games:
            # This is a simple way - you might need to add game_id to your data
            teams = df['team'].unique()
            unique_games = len(teams) // 2  # Rough estimate
        
        return jsonify({
            'status': 'success',
            'has_live_games': has_live_games,
            'player_count': player_count,
            'game_count': unique_games
        })
        
    except Exception as e:
        logging.error(f"Error checking live games status: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e),
            'has_live_games': False
        })

@app.route('/refresh-live')
def refresh_live_data():
    """Force a refresh of the live data"""
    try:
        result = update_live_games()
        
        if result is not None and not result.empty:
            return jsonify({'status': 'success', 'count': len(result)})
        elif result is None:
            # No live games - this is normal and should clear the data
            return jsonify({'status': 'success', 'message': 'No live games available', 'count': 0})
        else:
            return jsonify({'status': 'success', 'message': 'No players with stats in live games', 'count': 0})
            
    except Exception as e:
        logging.error(f"Error in refresh_live_data: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)})
    
if __name__ == '__main__':
    # Ensure templates and static directories exist
    os.makedirs('templates', exist_ok=True)
    os.makedirs('static/css', exist_ok=True)
    os.makedirs('static/js', exist_ok=True)
    
    # Run the initial update
    update_top_scorers()
    
    # Start the scheduler
    scheduler.start()

    # Start the live scheduler
    live_scheduler.start()
    
    # Start the Flask app
    app.run(debug=True, host='0.0.0.0', port=8080)

# If running with gunicorn in production (Render.com), these need to run outside __main__
else:
    # Ensure directories exist
    os.makedirs('templates', exist_ok=True)
    os.makedirs('static/css', exist_ok=True)
    os.makedirs('static/js', exist_ok=True)
    
    # Initialize database
    init_db()
    
    # Perform database migration
    migration_result = migrate_database()
    if migration_result:
        logging.info("Database migration completed successfully")
    else:
        logging.error("Database migration failed")
    
    # Clear live data on startup
    clear_live_data()
    logging.info("Cleared live data on startup")
    
    # Run the initial update
    update_top_scorers()
    
    # Start the schedulers
    scheduler.start()
    live_scheduler.start()
