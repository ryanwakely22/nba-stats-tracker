// Add these variables at the top of your script.js file
let currentSortColumn = 'custom_score'; // Default sort column
let currentSortDirection = 'desc';      // Default direction (descending)

document.addEventListener('DOMContentLoaded', function() {
    // Store fetched data globally
    let completedGamesData = [];
    let liveGamesData = [];
    
    // Initial data load
    fetchAllPlayerData();
    
    // Set up refresh button
    document.getElementById('refresh-button').addEventListener('click', function() {
        document.getElementById('loading-message').style.display = 'block';
        document.getElementById('players-table').style.display = 'none';
        document.getElementById('top-performers-chart-container').style.display = 'none';
        
        // Determine which refresh to use based on current view
        const toggleState = document.getElementById('data-toggle');
        const endpoint = (toggleState && toggleState.checked) ? '/refresh-live' : '/refresh';
        
        fetch(endpoint)
            .then(response => response.json())
            .then(data => {
                if (data.status === 'success') {
                    fetchAllPlayerData();
                } else {
                    document.getElementById('loading-message').textContent = 'Error refreshing data: ' + data.message;
                }
            })
            .catch(error => {
                document.getElementById('loading-message').textContent = 'Error refreshing data: ' + error;
            });
    });

    
    // Set up the toggle switch
    document.getElementById('data-toggle').addEventListener('change', function() {
        displaySelectedData();
    });
    
    // Set up column header click event listeners for sorting
    setupSortableColumns();
    
    // Update last update time
    fetchLastUpdateTime();
    
    // Auto-refresh every 5 minutes
    setInterval(fetchAllPlayerData, 5 * 60 * 1000);
    
    // Auto-refresh live data every 30 seconds
    setInterval(function() {
        fetch('/refresh-live')
            .then(response => response.json())
            .then(data => {
                if (data.status === 'success') {
                    fetchLiveGamesData();
                    console.log('Auto-refresh of live data completed');
                }
            })
            .catch(error => {
                console.error('Error during auto-refresh of live data:', error);
            });
    }, 30 * 1000);  // 30 seconds
    
    // Auto-refresh last update time every minute
    setInterval(fetchLastUpdateTime, 60 * 1000);
});

function setupSortableColumns() {
    // Get all the column headers
    const headers = document.querySelectorAll('#players-table thead th');
    
    // Define a mapping from column display names to data property names
    const columnMapping = {
        '#': 'rank',
        'Player': 'player_name',
        'Team': 'team',
        'MIN': 'minutes',
        'PTS': 'points',
        'REB': 'rebounds',
        'AST': 'assists',
        'STL': 'steals',
        'BLK': 'blocks',
        'TO': 'turnovers',
        'FGM': 'field_goal_made',
        'FGA': 'field_goal_attempts',
        '3PM': 'three_point_made',
        '3PA': 'three_point_attempts',
        'PF': 'personal_fouls',
        '+/-': 'plus_minus',
        'EPA': 'custom_score'
    };
    
    // Add click event listeners to all column headers
    headers.forEach(header => {
        const columnName = header.textContent.trim().replace(/[^\w\s/-]/g, ''); // Updated regex to preserve +/-
        
        // Get the data property name from the mapping, or use the column name if not found
        const dataProperty = columnMapping[columnName] || columnName.toLowerCase().replace(/\s+/g, '_');
        
        // Skip columns that shouldn't be sortable
        if (dataProperty === 'rank') return;
        
        // Add cursor pointer and hover effect
        header.style.cursor = 'pointer';
        header.classList.add('sortable');
        
        // Add the sort icon (initially hidden for all except custom_score)
        const sortIcon = document.createElement('span');
        sortIcon.classList.add('sort-icon');
        sortIcon.innerHTML = dataProperty === 'custom_score' ? ' ↓' : '';
        header.appendChild(sortIcon);
        
        // Add click event
        header.addEventListener('click', function() {
            sortTable(dataProperty);
            
            // Update the sort icons
            document.querySelectorAll('.sort-icon').forEach(icon => {
                icon.innerHTML = '';
            });
            
            // Show the appropriate icon based on sort direction
            sortIcon.innerHTML = currentSortDirection === 'asc' ? ' ↑' : ' ↓';
        });
    });
}

function fetchLastUpdateTime() {
    fetch('/api/last-update')
        .then(response => response.json())
        .then(data => {
            if (data.status === 'success') {
                const lastUpdateElement = document.getElementById('last-update-time');
                if (lastUpdateElement) {
                    // Format the timestamp for display
                    const timestamp = new Date(data.last_update);
                    lastUpdateElement.textContent = timestamp.toLocaleString();
                }
            }
        })
        .catch(error => {
            console.error('Error fetching last update time:', error);
        });
}

function fetchAllPlayerData() {
    // Fetch both completed games and live games data
    Promise.all([
        fetch('/api/top-scorers').then(response => response.json()),
        fetch('/api/live-games').then(response => response.json())
    ])
    .then(([completedData, liveData]) => {
        // Store the fetched data globally
        if (completedData.status === 'success' && completedData.players.length > 0) {
            // Filter out players with 0 minutes
            completedGamesData = completedData.players.filter(player => player.minutes !== "0:00" && player.minutes !== "0" && player.minutes !== "");
        } else {
            completedGamesData = [];
        }
        
        if (liveData.status === 'success' && liveData.players.length > 0) {
            // Filter out players with 0 minutes, then mark live game players for display purposes
            liveGamesData = liveData.players
                .filter(player => player.minutes !== "0:00" && player.minutes !== "0" && player.minutes !== "")
                .map(player => ({...player, isLive: true}));
            
            // Show toggle switch and live indicator if there's live data
            const toggleContainer = document.getElementById('toggle-container');
            const liveIndicator = document.getElementById('live-indicator');
            
            if (toggleContainer) {
                toggleContainer.style.display = liveGamesData.length > 0 ? 'flex' : 'none';
            }
            
            if (liveIndicator) {
                liveIndicator.style.display = liveGamesData.length > 0 ? 'block' : 'none';
            }
            
            // Auto-select live data if it's available
            if (liveGamesData.length > 0 && completedGamesData.length > 0) {
                document.getElementById('data-toggle').checked = true;
            }
        } else {
            liveGamesData = [];
            // Hide toggle switch and live indicator if there's no live data
            const toggleContainer = document.getElementById('toggle-container');
            const liveIndicator = document.getElementById('live-indicator');
            
            if (toggleContainer) {
                toggleContainer.style.display = 'none';
            }
            
            if (liveIndicator) {
                liveIndicator.style.display = 'none';
            }
            
            // Reset toggle to completed games if no live data
            document.getElementById('data-toggle').checked = false;
        }
        
        // Display the data based on toggle state
        displaySelectedData();
        
        // Hide loading message
        document.getElementById('loading-message').style.display = 'none';
    })
    .catch(error => {
        document.getElementById('loading-message').textContent = 'Error loading data: ' + error;
    });
}

function fetchLiveGamesData() {
    // Only refresh live games data
    fetch('/api/live-games')
        .then(response => response.json())
        .then(liveData => {
            if (liveData.status === 'success' && liveData.players.length > 0) {
                // Filter out players with 0 minutes, then mark live game players for display purposes
                liveGamesData = liveData.players
                    .filter(player => player.minutes !== "0:00" && player.minutes !== "0" && player.minutes !== "")
                    .map(player => ({...player, isLive: true}));
                
                // Show toggle switch and live indicator if there are players with minutes > 0
                const toggleContainer = document.getElementById('toggle-container');
                const liveIndicator = document.getElementById('live-indicator');
                
                if (toggleContainer) {
                    toggleContainer.style.display = liveGamesData.length > 0 ? 'flex' : 'none';
                }
                
                if (liveIndicator) {
                    liveIndicator.style.display = liveGamesData.length > 0 ? 'block' : 'none';
                }
                
                // If toggle is set to live games, update the display
                if (document.getElementById('data-toggle').checked) {
                    displaySelectedData();
                }
            } else {
                liveGamesData = [];
                
                // Hide toggle switch and live indicator if there's no live data
                const toggleContainer = document.getElementById('toggle-container');
                const liveIndicator = document.getElementById('live-indicator');
                
                if (toggleContainer) {
                    toggleContainer.style.display = 'none';
                }
                
                if (liveIndicator) {
                    liveIndicator.style.display = 'none';
                }
                
                // If toggle was set to live games but now there are none, switch to completed games
                if (document.getElementById('data-toggle').checked && completedGamesData.length > 0) {
                    document.getElementById('data-toggle').checked = false;
                    displaySelectedData();
                }
            }
        })
        .catch(error => {
            console.error('Error fetching live games data:', error);
        });
}

function sortTable(column) {
    // If clicking the same column, reverse the direction
    if (column === currentSortColumn) {
        currentSortDirection = currentSortDirection === 'asc' ? 'desc' : 'asc';
    } else {
        // New column, set default direction (descending for numeric, ascending for text)
        const numericColumns = ['points', 'rebounds', 'assists', 
                              'steals', 'blocks', 'turnovers', 'field_goal_made', 'field_goal_attempts',
                              'three_point_made', 'three_point_attempts', 'personal_fouls', 'plus_minus', 'custom_score'];
        
        currentSortDirection = numericColumns.includes(column) ? 'desc' : 'asc';
        currentSortColumn = column;
    }
    
    // Refresh the display with new sort parameters
    displaySelectedData();
}

function displaySelectedData() {
    const showLiveData = document.getElementById('data-toggle').checked;
    let playersToDisplay = [];
    
    if (showLiveData && liveGamesData.length > 0) {
        // Show live games data
        playersToDisplay = liveGamesData;
        
        // Update header text
        const headerText = document.getElementById('header-description');
        if (headerText) {
            headerText.textContent = 'Top performers from live games';
        }
    } else {
        // Show completed games data
        playersToDisplay = completedGamesData;
        
        // Update header text
        const headerText = document.getElementById('header-description');
        if (headerText) {
            headerText.textContent = 'Top performers from games in the last 12 hours';
        }
        
        // If no completed games data is available
        if (playersToDisplay.length === 0) {
            document.getElementById('loading-message').textContent = 'No completed games data available';
            document.getElementById('loading-message').style.display = 'block';
            document.getElementById('players-table').style.display = 'none';
            document.getElementById('top-performers-chart-container').style.display = 'none';
            return;
        }
    }
    
    // Sort players based on current sort column and direction
    playersToDisplay.sort((a, b) => {
        let aValue = a[currentSortColumn];
        let bValue = b[currentSortColumn];
        
        // Convert to numbers for numeric columns
        if (typeof aValue === 'string' && !isNaN(parseFloat(aValue))) {
            aValue = parseFloat(aValue);
            bValue = parseFloat(bValue);
        }
        
        // Special handling for minutes which is in format "MM:SS"
        if (currentSortColumn === 'minutes') {
            aValue = convertMinutesToSeconds(aValue);
            bValue = convertMinutesToSeconds(bValue);
        }
        
        // Compare based on direction
        if (currentSortDirection === 'asc') {
            if (aValue < bValue) return -1;
            if (aValue > bValue) return 1;
            return 0;
        } else {
            if (aValue > bValue) return -1;
            if (aValue < bValue) return 1;
            return 0;
        }
    });
    
    // Update UI
    populateTable(playersToDisplay);
    createChart(playersToDisplay.slice(0, 10));
    
    document.getElementById('players-table').style.display = 'table';
    document.getElementById('top-performers-chart-container').style.display = 'block';
}

function convertMinutesToSeconds(minutesStr) {
    if (!minutesStr) return 0;
    
    try {
        const parts = minutesStr.split(':');
        if (parts.length === 2) {
            return parseInt(parts[0]) * 60 + parseInt(parts[1]);
        }
        return parseInt(minutesStr) * 60;
    } catch (e) {
        console.error('Error converting minutes to seconds:', minutesStr);
        return 0;
    }
}

function populateTable(players) {
    const tableBody = document.getElementById('player-data');
    tableBody.innerHTML = '';
    
    players.forEach((player, index) => {
        const row = document.createElement('tr');
        
        // Add "LIVE" class to rows with live game data
        if (player.isLive) {
            row.classList.add('live-game');
        }
        
        row.innerHTML = `
            <td>${index + 1}</td>
            <td><strong>${player.player_name}</strong>${player.isLive ? ' <span class="live-badge">LIVE</span>' : ''}</td>
            <td>${player.team}</td>
            <td>${player.minutes}</td>
            <td>${player.points}</td>
            <td>${player.rebounds}</td>
            <td>${player.assists}</td>
            <td>${player.steals}</td>
            <td>${player.blocks}</td>
            <td>${player.turnovers}</td>
            <td>${player.field_goal_made}</td>
            <td>${player.field_goal_attempts}</td>
            <td>${player.three_point_made}</td>
            <td>${player.three_point_attempts}</td>
            <td>${player.personal_fouls}</td>
            <td>${player.plus_minus > 0 ? '+' + player.plus_minus : player.plus_minus}</td>
            <td><strong>${player.custom_score.toFixed(2)}</strong></td>
        `;
        
        tableBody.appendChild(row);
    });
}

function createChart(players) {
    // Reverse the array to display highest score at the top
    const reversedPlayers = [...players].reverse();
    
    const ctx = document.getElementById('top-performers-chart').getContext('2d');
    
    // Destroy existing chart if it exists
    if (window.topPerformersChart) {
        window.topPerformersChart.destroy();
    }
    
    window.topPerformersChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: reversedPlayers.map(player => `${player.player_name} (${player.team})`),
            datasets: [{
                label: 'Custom Score',
                data: reversedPlayers.map(player => player.custom_score),
                backgroundColor: reversedPlayers.map(player => player.isLive ? 'rgba(255, 77, 77, 0.6)' : 'rgba(54, 162, 235, 0.6)'),
                borderColor: reversedPlayers.map(player => player.isLive ? 'rgba(255, 77, 77, 1)' : 'rgba(54, 162, 235, 1)'),
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            indexAxis: 'y',  // This makes the bar chart horizontal
            scales: {
                x: {
                    beginAtZero: true
                }
            }
        }
    });
}