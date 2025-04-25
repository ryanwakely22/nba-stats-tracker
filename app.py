from flask import Flask, render_template, jsonify, request
from database import init_db, get_latest_scorers, get_last_update_time
from data_processor import update_top_scorers
from apscheduler.schedulers.background import BackgroundScheduler
import pandas as pd
import logging
import os
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='app.log'
)

app = Flask(__name__)

# Initialize database
init_db()

# Create a scheduler for automatic updates
scheduler = BackgroundScheduler()
scheduler.add_job(update_top_scorers, 'interval', minutes=3)

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
            players.append({
                'player_name': row['player_name'],
                'team': row['team'],
                'minutes': row['minutes'],
                'points': int(row['points']),
                'rebounds': int(row['rebounds']),
                'assists': int(row['assists']),
                'steals': int(row['steals']),
                'blocks': int(row['blocks']),
                'turnovers': int(row['turnovers']),
                'field_goal_attempts': int(row['field_goal_attempts']),
                'free_throw_attempts': int(row['free_throw_attempts']),
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

if __name__ == '__main__':
    # Ensure templates and static directories exist
    os.makedirs('templates', exist_ok=True)
    os.makedirs('static/css', exist_ok=True)
    os.makedirs('static/js', exist_ok=True)
    
    # Run the initial update
    update_top_scorers()
    
    # Start the scheduler
    scheduler.start()
    
    # Start the Flask app
    app.run(debug=True, host='0.0.0.0', port=8080)
