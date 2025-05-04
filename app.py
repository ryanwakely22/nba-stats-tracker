from flask import Flask, render_template, jsonify, request, Response
from database import init_db, get_latest_scorers, get_last_update_time, get_latest_live_data, clear_live_data
from data_processor import update_top_scorers, update_live_games
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from threading import Thread
import pandas as pd
import logging
import os
import csv
import io
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
        # Start update in background
        thread = Thread(target=update_top_scorers)
        thread.daemon = True
        thread.start()
        
        # Return immediately
        return jsonify({
            'status': 'processing',
            'message': 'Data refresh started in background'
        })
        
    except Exception as e:
        logging.error(f"Error starting refresh: {str(e)}")
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
        
        # Ensure we always return valid JSON
        if result is not None and not result.empty:
            return jsonify({'status': 'success', 'count': len(result)})
        elif result is None:
            return jsonify({'status': 'success', 'message': 'No live games available', 'count': 0})
        else:
            return jsonify({'status': 'success', 'message': 'No players with stats in live games', 'count': 0})
            
    except Exception as e:
        logging.error(f"Error in refresh_live_data: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        # Always return valid JSON, even in case of error
        return jsonify({'status': 'error', 'message': str(e)})

# Add this new route to your app.py
@app.route('/download-csv')
def download_csv():
    """Generate and download CSV file with current stats"""
    try:
        # Check if user wants live or completed games data
        data_type = request.args.get('type', 'completed')
        
        if data_type == 'live':
            df = get_latest_live_data()
        else:
            df = get_latest_scorers()
        
        if df.empty:
            return jsonify({'status': 'error', 'message': 'No data available'})
        
        # Create a string buffer to store CSV data
        output = io.StringIO()
        
        # Prepare data for export
        export_data = []
        for _, row in df.iterrows():
            export_data.append({
                'Player': row['player_name'],
                'Team': row['team'],
                'Minutes': row['minutes'],
                'Points': row['points'],
                'Off_Rebounds': row['offensive_rebounds'],
                'Def_Rebounds': row['defensive_rebounds'],
                'Total_Rebounds': row['offensive_rebounds'] + row['defensive_rebounds'],
                'Assists': row['assists'],
                'Steals': row['steals'],
                'Blocks': row['blocks'],
                'Turnovers': row['turnovers'],
                'FG_Made': row['field_goal_made'],
                'FG_Attempts': row['field_goal_attempts'],
                'FG_Percentage': f"{(row['field_goal_made'] / row['field_goal_attempts'] * 100):.1f}%" if row['field_goal_attempts'] > 0 else "0.0%",
                'Three_Made': row['three_point_made'],
                'Three_Attempts': row['three_point_attempts'],
                'Three_Percentage': f"{(row['three_point_made'] / row['three_point_attempts'] * 100):.1f}%" if row['three_point_attempts'] > 0 else "0.0%",
                'Personal_Fouls': row['personal_fouls'],
                'Plus_Minus': row['plus_minus'],
                'EPA_Score': row['custom_score']
            })
        
        # Create CSV writer
        if export_data:
            fieldnames = export_data[0].keys()
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            
            # Write headers and data
            writer.writeheader()
            writer.writerows(export_data)
            
            # Create response
            output.seek(0)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"nba_stats_{data_type}_{timestamp}.csv"
            
            return Response(
                output.getvalue(),
                mimetype='text/csv',
                headers={'Content-Disposition': f'attachment; filename={filename}'}
            )
        else:
            return jsonify({'status': 'error', 'message': 'No data to export'})
            
    except Exception as e:
        logging.error(f"Error generating CSV: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)})

# Add this route to get data directly as JSON for sheets integration
@app.route('/api/sheets-data')
def sheets_data():
    """Get data formatted for Google Sheets"""
    try:
        data_type = request.args.get('type', 'completed')
        
        if data_type == 'live':
            df = get_latest_live_data()
        else:
            df = get_latest_scorers()
        
        if df.empty:
            return jsonify({'status': 'error', 'message': 'No data available'})
        
        # Format data for sheets
        sheets_data = []
        for _, row in df.iterrows():
            sheets_data.append([
                row['player_name'],
                row['team'],
                row['minutes'],
                int(row['points']),
                int(row['offensive_rebounds']),
                int(row['defensive_rebounds']),
                int(row['offensive_rebounds'] + row['defensive_rebounds']),
                int(row['assists']),
                int(row['steals']),
                int(row['blocks']),
                int(row['turnovers']),
                int(row['field_goal_made']),
                int(row['field_goal_attempts']),
                f"{(row['field_goal_made'] / row['field_goal_attempts'] * 100):.1f}" if row['field_goal_attempts'] > 0 else "0.0",
                int(row['three_point_made']),
                int(row['three_point_attempts']),
                f"{(row['three_point_made'] / row['three_point_attempts'] * 100):.1f}" if row['three_point_attempts'] > 0 else "0.0",
                int(row['personal_fouls']),
                int(row['plus_minus']),
                float(row['custom_score'])
            ])
        
        return jsonify({
            'status': 'success',
            'headers': ['Player', 'Team', 'Minutes', 'Points', 'OREB', 'DREB', 'Total REB', 
                       'Assists', 'Steals', 'Blocks', 'Turnovers', 'FGM', 'FGA', 'FG%', 
                       '3PM', '3PA', '3P%', 'Fouls', '+/-', 'EPA Score'],
            'data': sheets_data
        })
        
    except Exception as e:
        logging.error(f"Error getting sheets data: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/test-api-connection')
def test_api_connection():
    """Test endpoint to check API connectivity and environment"""
    import platform
    import requests
    
    results = {
        'platform': platform.platform(),
        'python_version': sys.version,
        'is_render': bool(os.environ.get('RENDER')),
        'environment_vars': {
            'PORT': os.environ.get('PORT'),
            'RENDER': os.environ.get('RENDER'),
            'IS_PULL_REQUEST': os.environ.get('IS_PULL_REQUEST')
        }
    }
    
    # Test NBA API connection
    try:
        response = requests.get('https://stats.nba.com/js/data/playermovement/NBA_Player_Movement.json',
                             headers={'User-Agent': 'Mozilla/5.0'}, 
                             timeout=10)
        results['nba_api_test'] = {
            'status': response.status_code,
            'success': response.status_code == 200,
            'response_time': response.elapsed.total_seconds()
        }
    except Exception as e:
        results['nba_api_test'] = {
            'success': False,
            'error': str(e)
        }
    
    # Test database connection
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
        table_count = cursor.fetchone()[0]
        conn.close()
        results['database_test'] = {
            'success': True,
            'table_count': table_count
        }
    except Exception as e:
        results['database_test'] = {
            'success': False,
            'error': str(e)
        }
    
    return jsonify(results)


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
