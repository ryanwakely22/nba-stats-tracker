document.addEventListener('DOMContentLoaded', function() {
    // Load players data when page loads
    fetchPlayersData();
    
    // Set up refresh button
    document.getElementById('refresh-btn').addEventListener('click', function() {
        refreshData();
    });
    
    // Automatically refresh data every 5 minutes
    setInterval(fetchPlayersData, 5 * 60 * 1000);
});

function fetchPlayersData() {
    const loadingAlert = document.getElementById('loading-alert');
    const errorAlert = document.getElementById('error-alert');
    
    loadingAlert.classList.remove('d-none');
    errorAlert.classList.add('d-none');
    
    fetch('/api/top-scorers')
        .then(response => response.json())
        .then(data => {
            loadingAlert.classList.add('d-none');
            
            if (data.status === 'success') {
                displayPlayers(data.players);
                updateLastUpdated();
            } else {
                errorAlert.textContent = 'Error: ' + data.message;
                errorAlert.classList.remove('d-none');
            }
        })
        .catch(error => {
            loadingAlert.classList.add('d-none');
            errorAlert.textContent = 'Error connecting to server: ' + error;
            errorAlert.classList.remove('d-none');
            console.error('Error fetching players data:', error);
        });
}

function refreshData() {
    const refreshBtn = document.getElementById('refresh-btn');
    refreshBtn.disabled = true;
    refreshBtn.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Refreshing...';
    
    fetch('/refresh')
        .then(response => response.json())
        .then(data => {
            if (data.status === 'success') {
                fetchPlayersData();
            } else {
                alert('Failed to refresh data: ' + data.message);
            }
        })
        .catch(error => {
            console.error('Error refreshing data:', error);
            alert('An error occurred while refreshing data.');
        })
        .finally(() => {
            refreshBtn.disabled = false;
            refreshBtn.textContent = 'Refresh Data';
        });
}

function displayPlayers(players) {
    const tableBody = document.getElementById('players-table');
    
    if (!players || players.length === 0) {
        tableBody.innerHTML = '<tr><td colspan="13" class="text-center">No player data available for the last 12 hours.</td></tr>';
        return;
    }
    
    let html = '';
    players.forEach((player, index) => {
        // Calculate text color for custom score (green for positive, red for negative)
        const scoreColor = player.custom_score >= 0 ? 'text-success' : 'text-danger';
        
        html += `
            <tr>
                <td>${index + 1}</td>
                <td><strong>${player.player_name}</strong></td>
                <td>${player.team}</td>
                <td>${player.minutes}</td>
                <td>${player.points}</td>
                <td>${player.rebounds}</td>
                <td>${player.assists}</td>
                <td>${player.steals}</td>
                <td>${player.blocks}</td>
                <td>${player.turnovers}</td>
                <td>${player.field_goal_attempts}</td>
                <td>${player.free_throw_attempts}</td>
                <td class="custom-score ${scoreColor}">${player.custom_score.toFixed(1)}</td>
            </tr>
        `;
    });
    
    tableBody.innerHTML = html;
}

function updateLastUpdated() {
    fetch('/api/last-update')
        .then(response => response.json())
        .then(data => {
            if (data.status === 'success') {
                const timestamp = new Date(data.last_update);
                document.getElementById('last-updated').textContent = timestamp.toLocaleString();
            }
        })
        .catch(error => {
            console.error('Error fetching last update time:', error);
        });
}