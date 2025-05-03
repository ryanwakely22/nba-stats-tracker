// Add these variables at the top of your script.js file
let currentSortColumn = 'custom_score'; // Default sort column
let currentSortDirection = 'desc';      // Default direction (descending)
let completedGamesData = [];
let liveGamesData = [];

document.addEventListener('DOMContentLoaded', function() {
    console.log("Initializing player stats dashboard with dynamic live games");
    
    // Initial data load
    fetchAllPlayerData();
    
    // Set up toggle functionality
    setupToggleButtons();
    
    // Show the default view (top scorers)
    showView('topScorersView');
    
    // Set up refresh button
    document.getElementById('refresh-button').addEventListener('click', function() {
        console.log("Manual refresh triggered");
        refreshData();
    });
    
    // Set up column header click event listeners for sorting
    setupSortableColumns();
    
    // Update last update time
    fetchLastUpdateTime();
    
    // Auto-refresh every 5 minutes
    setInterval(fetchAllPlayerData, 5 * 60 * 1000);
    
    // Auto-refresh live data every 30 seconds
    setInterval(checkAndUpdateLiveGames, 30 * 1000);
    
    // Auto-refresh last update time every minute
    setInterval(fetchLastUpdateTime, 60 * 1000);
});

function setupToggleButtons() {
    console.log("Setting up toggle buttons");
    
    // Top Scorers button
    const topScorersButton = document.getElementById('topScorersButton');
    if (topScorersButton) {
        topScorersButton.addEventListener('click', function() {
            console.log("Top Scorers view selected");
            showView('topScorersView');
            setActiveButton(topScorersButton);
        });
    }
    
    // Live Games button
    const liveGamesButton = document.getElementById('liveGamesButton');
    if (liveGamesButton) {
        liveGamesButton.addEventListener('click', function() {
            console.log("Live Games view selected");
            showView('liveGamesView');
            setActiveButton(liveGamesButton);
            // Refresh live games data when switching to this view
            checkAndUpdateLiveGames();
        });
    }
}

function showView(viewId) {
    console.log(`Showing view: ${viewId}`);
    
    // Hide all views
    const views = ['topScorersView', 'liveGamesView'];
    views.forEach(id => {
        const element = document.getElementById(id);
        if (element) {
            element.style.display = 'none';
        }
    });
    
    // Show the selected view
    const selectedView = document.getElementById(viewId);
    if (selectedView) {
        selectedView.style.display = 'block';
    }
    
    // Update header description
    const headerDescription = document.getElementById('header-description');
    if (headerDescription) {
        if (viewId === 'topScorersView') {
            headerDescription.textContent = 'Top performers from games in the last 12 hours';
        } else if (viewId === 'liveGamesView') {
            headerDescription.textContent = 'Top performers from live games';
        }
    }
    
    // Update data display
    displaySelectedData(viewId);
}

function setActiveButton(activeButton) {
    console.log(`Setting active button: ${activeButton.id}`);
    
    // Remove active class from all toggle buttons
    const buttons = document.querySelectorAll('.toggle-button');
    buttons.forEach(button => {
        button.classList.remove('active');
    });
    
    // Add active class to the selected button
    activeButton.classList.add('active');
}

function checkAndUpdateLiveGames() {
    console.log("Checking for live games...");
    fetch('/api/live-games')
        .then(response => response.json())
        .then(data => {
            const liveGamesButton = document.getElementById('liveGamesButton');
            
            if (data.status === 'success' && data.players && data.players.length > 0) {
                // Filter out players with 0 minutes
                liveGamesData = data.players
                    .filter(player => player.minutes !== "0:00" && player.minutes !== "0" && player.minutes !== "")
                    .map(player => ({...player, isLive: true}));
                
                // Show the live games button if there are players with actual minutes
                if (liveGamesButton && liveGamesData.length > 0) {
                    liveGamesButton.style.display = 'inline-block';
                    
                    // Update the live games title with game count
                    try {
                        const uniqueTeams = new Set(liveGamesData.map(player => player.team));
                        const approximateGames = Math.ceil(uniqueTeams.size / 2);
                        const liveGamesTitle = document.getElementById('live-games-title');
                        if (liveGamesTitle) {
                            liveGamesTitle.textContent = `Live Games (${approximateGames} game${approximateGames !== 1 ? 's' : ''} in progress)`;
                        }
                    } catch (e) {
                        console.error('Error calculating game count:', e);
                    }
                    
                    // If we're currently viewing live games, update the display
                    if (liveGamesButton.classList.contains('active')) {
                        displaySelectedData('liveGamesView');
                    }
                } else {
                    // No players with actual minutes - hide the button
                    hideLiveGamesButton();
                }
                
            } else {
                // No live games - hide the button
                liveGamesData = [];
                hideLiveGamesButton();
            }
        })
        .catch(error => {
            console.error('Error fetching live games:', error);
            hideLiveGamesButton();
        });
}

function hideLiveGamesButton() {
    const liveGamesButton = document.getElementById('liveGamesButton');
    const topScorersButton = document.getElementById('topScorersButton');
    
    if (liveGamesButton) {
        liveGamesButton.style.display = 'none';
        
        // If we're currently viewing live games, switch to top scorers
        if (liveGamesButton.classList.contains('active') && topScorersButton) {
            topScorersButton.click();
        }
    }
}

function fetchAllPlayerData() {
    // Show loading message
    document.getElementById('loading-message').style.display = 'block';
    
    // Fetch both completed games and live games data
    Promise.all([
        fetch('/api/top-scorers').then(response => response.json()),
        fetch('/api/live-games').then(response => response.json())
    ])
    .then(([completedData, liveData]) => {
        // Store completed games data
        if (completedData.status === 'success' && completedData.players.length > 0) {
            completedGamesData = completedData.players.filter(player => 
                player.minutes !== "0:00" && player.minutes !== "0" && player.minutes !== "");
        } else {
            completedGamesData = [];
        }
        
        // Handle live games data
        checkAndUpdateLiveGames();
        
        // Display the data based on current view
        const activeView = document.querySelector('.toggle-button.active');
        if (activeView) {
            if (activeView.id === 'topScorersButton') {
                displaySelectedData('topScorersView');
            } else if (activeView.id === 'liveGamesButton' && liveGamesData.length > 0) {
                displaySelectedData('liveGamesView');
            }
        } else {
            displaySelectedData('topScorersView');
        }
        
        // Hide loading message
        document.getElementById('loading-message').style.display = 'none';
    })
    .catch(error => {
        console.error('Error loading data:', error);
        document.getElementById('loading-message').textContent = 'Error loading data: ' + error;
    });
}

function setupSortableColumns() {
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
    
    // Add sortable functionality to both tables
    ['players-table', 'live-players-table'].forEach(tableId => {
        const table = document.getElementById(tableId);
        if (table) {
            const headers = table.querySelectorAll('thead th');
            headers.forEach(header => {
                const columnName = header.textContent.trim();
                const dataProperty = columnMapping[columnName] || columnName.toLowerCase().replace(/\s+/g, '_');
                
                // Skip columns that shouldn't be sortable
                if (dataProperty === '#' || dataProperty === 'rank') return;
                
                // Add cursor pointer and hover effect
                header.style.cursor = 'pointer';
                header.classList.add('sortable');
                
                // Add the sort icon
                const sortIcon = document.createElement('span');
                sortIcon.classList.add('sort-icon');
                sortIcon.innerHTML = dataProperty === 'custom_score' ? ' ↓' : '';
                header.appendChild(sortIcon);
                
                // Add click event
                header.addEventListener('click', function() {
                    sortTable(dataProperty);
                    
                    // Update the sort icons
                    table.querySelectorAll('.sort-icon').forEach(icon => {
                        icon.innerHTML = '';
                    });
                    
                    // Show the appropriate icon based on sort direction
                    sortIcon.innerHTML = currentSortDirection === 'asc' ? ' ↑' : ' ↓';
                });
            });
        }
    });
}

function fetchLastUpdateTime() {
    fetch('/api/last-update')
        .then(response => response.json())
        .then(data => {
            if (data.status === 'success') {
                const lastUpdateElement = document.getElementById('last-update-time');
                if (lastUpdateElement) {
                    const timestamp = new Date(data.last_update);
                    lastUpdateElement.textContent = timestamp.toLocaleString();
                }
            }
        })
        .catch(error => {
            console.error('Error fetching last update time:', error);
        });
}

function sortTable(column) {
    // If clicking the same column, reverse the direction
    if (column === currentSortColumn) {
        currentSortDirection = currentSortDirection === 'asc' ? 'desc' : 'asc';
    } else {
        // New column, set default direction
        const numericColumns = ['points', 'rebounds', 'assists', 'steals', 'blocks', 
                              'turnovers', 'field_goal_made', 'field_goal_attempts',
                              'three_point_made', 'three_point_attempts', 'personal_fouls', 
                              'plus_minus', 'custom_score'];
        
        currentSortDirection = numericColumns.includes(column) ? 'desc' : 'asc';
        currentSortColumn = column;
    }
    
    // Refresh the display with new sort parameters
    const activeView = document.querySelector('.toggle-button.active');
    if (activeView) {
        const viewId = activeView.id === 'topScorersButton' ? 'topScorersView' : 'liveGamesView';
        displaySelectedData(viewId);
    }
}

function displaySelectedData(viewId) {
    let playersToDisplay = [];
    let tableId = '';
    let tableBodyId = '';
    
    if (viewId === 'topScorersView') {
        playersToDisplay = completedGamesData;
        tableId = 'players-table';
        tableBodyId = 'player-data';
        
        // Show no data message if needed
        if (playersToDisplay.length === 0) {
            document.getElementById('loading-message').textContent = 'No completed games data available';
            document.getElementById('loading-message').style.display = 'block';
            document.getElementById('top-performers-chart-container').style.display = 'none';
            return;
        }
    } else if (viewId === 'liveGamesView') {
        playersToDisplay = liveGamesData;
        tableId = 'live-players-table';
        tableBodyId = 'live-player-data';
        
        // Show no live games message if needed
        if (playersToDisplay.length === 0) {
            const tableBody = document.getElementById(tableBodyId);
            if (tableBody) {
                tableBody.innerHTML = '<tr><td colspan="17" class="no-games-message">No live games at the moment. Check back during game time!</td></tr>';
            }
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
        
        // Special handling for minutes
        if (currentSortColumn === 'minutes') {
            aValue = convertMinutesToSeconds(aValue);
            bValue = convertMinutesToSeconds(bValue);
        }
        
        // Handle plus_minus values
        if (currentSortColumn === 'plus_minus') {
            aValue = parseInt(aValue) || 0;
            bValue = parseInt(bValue) || 0;
        }
        
        // Compare based on direction
        if (currentSortDirection === 'asc') {
            return aValue < bValue ? -1 : aValue > bValue ? 1 : 0;
        } else {
            return aValue > bValue ? -1 : aValue < bValue ? 1 : 0;
        }
    });
    
    // Hide loading message
    document.getElementById('loading-message').style.display = 'none';
    
    // Update table
    populateTable(playersToDisplay, tableBodyId);
    
    // Update chart
    createChart(playersToDisplay.slice(0, 10));
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

function populateTable(players, tableBodyId) {
    const tableBody = document.getElementById(tableBodyId);
    if (!tableBody) return;
    
    tableBody.innerHTML = '';
    
    players.forEach((player, index) => {
        const row = document.createElement('tr');
        
        // Add live game class if needed
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

function refreshData() {
    const activeView = document.querySelector('.toggle-button.active');
    const viewType = activeView && activeView.id === 'liveGamesButton' ? 'live' : 'top';
    
    document.getElementById('loading-message').textContent = 'Refreshing data...';
    document.getElementById('loading-message').style.display = 'block';
    
    if (viewType === 'live') {
        fetch('/refresh-live')
            .then(response => response.json())
            .then(data => {
                if (data.status === 'success') {
                    checkAndUpdateLiveGames();
                } else {
                    document.getElementById('loading-message').textContent = 'Error refreshing live data: ' + data.message;
                }
            })
            .catch(error => {
                document.getElementById('loading-message').textContent = 'Error refreshing live data: ' + error;
            });
    } else {
        fetch('/refresh')
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
    }
}