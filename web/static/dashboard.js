// Dashboard JavaScript for Cloud Sync Monitoring
class CloudSyncDashboard {
    constructor() {
        this.websocket = null;
        this.refreshInterval = null;
        this.currentPage = 1;
        this.logsPerPage = 50;
        this.filters = {
            stage: '',
            status: '',
            search: ''
        };
        
        this.init();
    }

    init() {
        this.hideLoadingOverlay();
        this.setupEventListeners();
        this.startAutoRefresh();
        this.connectWebSocket();
        this.loadInitialData();
    }

    setupEventListeners() {
        // Manual refresh button
        document.getElementById('refreshBtn').addEventListener('click', () => {
            this.refreshAllData();
        });

        // Control buttons
        document.getElementById('restartBtn').addEventListener('click', () => {
            this.executeControl('restart');
        });
        
        document.getElementById('pauseBtn').addEventListener('click', () => {
            this.executeControl('pause');
        });
        
        document.getElementById('resumeBtn').addEventListener('click', () => {
            this.executeControl('resume');
        });



        // Log filters
        document.getElementById('logSearch').addEventListener('input', (e) => {
            this.filters.search = e.target.value;
            this.debounce(() => this.loadLogs(), 300);
        });
        
        document.getElementById('stageFilter').addEventListener('change', (e) => {
            this.filters.stage = e.target.value;
            this.loadLogs();
        });
        
        document.getElementById('statusFilter').addEventListener('change', (e) => {
            this.filters.status = e.target.value;
            this.loadLogs();
        });

        // Pagination
        document.getElementById('prevPage').addEventListener('click', () => {
            if (this.currentPage > 1) {
                this.currentPage--;
                this.loadLogs();
            }
        });
        
        document.getElementById('nextPage').addEventListener('click', () => {
            this.currentPage++;
            this.loadLogs();
        });

        // Modal close handlers
        document.querySelectorAll('.modal-close').forEach(btn => {
            btn.addEventListener('click', () => {
                this.hideModal();
            });
        });
    }



    connectWebSocket() {
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${window.location.host}/ws`;
        
        this.websocket = new WebSocket(wsUrl);
        
        this.websocket.onopen = () => {
            console.log('WebSocket connected');
            this.updateConnectionStatus('cloudSync', 'connected');
        };
        
        this.websocket.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);
                this.handleWebSocketMessage(data);
            } catch (error) {
                console.error('Error parsing WebSocket message:', error);
            }
        };
        
        this.websocket.onclose = () => {
            console.log('WebSocket disconnected');
            this.updateConnectionStatus('cloudSync', 'disconnected');
            // Attempt to reconnect after 5 seconds
            setTimeout(() => this.connectWebSocket(), 5000);
        };
        
        this.websocket.onerror = (error) => {
            console.error('WebSocket error:', error);
            this.updateConnectionStatus('cloudSync', 'warning');
        };
    }

    handleWebSocketMessage(data) {
        // Handle real-time updates from WebSocket
        if (data.type === 'metrics_update') {
            this.updateMetrics(data.metrics);
        } else if (data.type === 'status_update') {
            this.updateConnectionStatus(data.component, data.status);
        } else if (data.type === 'log_entry') {
            this.addLogEntry(data.log);
        }
    }

    async loadInitialData() {
        try {
            await Promise.all([
                this.loadMetrics(),
                this.loadHealthStatus(),
                this.loadLogs()
            ]);
        } catch (error) {
            console.error('Error loading initial data:', error);
            this.showError('Failed to load dashboard data');
        }
    }

    async loadMetrics() {
        try {
            const response = await fetch('/api/metrics');
            if (!response.ok) throw new Error('Failed to fetch metrics');
            
            const data = await response.json();
            this.updateMetrics(data);
        } catch (error) {
            console.error('Error loading metrics:', error);
        }
    }

    async loadHealthStatus() {
        try {
            const response = await fetch('/health');
            if (!response.ok) throw new Error('Failed to fetch health status');
            
            const data = await response.json();
            this.updateHealthStatus(data);
        } catch (error) {
            console.error('Error loading health status:', error);
        }
    }

    async loadLogs() {
        try {
            const params = new URLSearchParams({
                page: this.currentPage,
                limit: this.logsPerPage,
                stage: this.filters.stage,
                status: this.filters.status,
                search: this.filters.search
            });
            
            const response = await fetch(`/api/logs?${params}`);
            if (!response.ok) throw new Error('Failed to fetch logs');
            
            const data = await response.json();
            this.updateLogsTable(data);
        } catch (error) {
            console.error('Error loading logs:', error);
        }
    }



    updateMetrics(metrics) {
        // Check if we have dashboard_metrics from the new API structure
        const dashboardMetrics = metrics.dashboard_metrics || metrics;
        
        // Update statistics cards using dashboard_metrics
        this.updateElement('totalDocs', this.formatNumber(dashboardMetrics.total_documents || 0));
        this.updateElement('todayDocs', this.formatNumber(dashboardMetrics.total_documents || 0)); // Use same as total for now
        this.updateElement('syncRate', this.formatNumber(dashboardMetrics.sync_rate || 0, 1));
        this.updateElement('backlogSize', this.formatNumber(dashboardMetrics.backlog_size || 0));
        this.updateElement('avgLatency', this.formatNumber(dashboardMetrics.avg_latency || 0, 1));
        this.updateElement('activeWatchers', dashboardMetrics.active_watchers || 0);
        
        // Update config info (fallback to original metrics structure)
        this.updateElement('lastResumeToken', this.formatTimestamp(metrics.last_resume_token));
        this.updateElement('syncMode', metrics.sync_mode || 'Unknown');
        this.updateElement('lastCheckpoint', this.formatTimestamp(metrics.last_checkpoint));
        this.updateElement('connectedClients', metrics.connected_clients || 0);
    }

    updateHealthStatus(health) {
        this.updateConnectionStatus('sourceMongo', health.source_mongo || 'unknown');
        this.updateConnectionStatus('cloudSync', health.cloud_sync || 'unknown');
        this.updateConnectionStatus('vmSync', health.vm_sync || 'unknown');
        this.updateConnectionStatus('targetMongo', health.target_mongo || 'unknown');
        
        // Update VM sync details if available
        if (health.vm_sync_info) {
            this.updateVMSyncDetails(health.vm_sync_info);
        }
    }

    updateConnectionStatus(component, status) {
        const card = document.getElementById(component);
        if (!card) return;
        
        // Remove existing status classes
        card.classList.remove('connected', 'disconnected', 'warning');
        
        // Add new status class
        card.classList.add(status);
        
        // Update status text
        const statusText = card.querySelector('.status-text');
        if (statusText) {
            statusText.textContent = this.getStatusText(status);
        }
    }

    updateVMSyncDetails(vmSyncInfo) {
        const addressElement = document.getElementById('vmSyncAddress');
        const clientsElement = document.getElementById('vmSyncClients');
        
        if (addressElement) {
            addressElement.textContent = `Server: ${vmSyncInfo.server_address || 'N/A'}`;
        }
        
        if (clientsElement) {
            const clientCount = vmSyncInfo.connected_clients || 0;
            clientsElement.textContent = `Connected clients: ${clientCount}`;
        }
    }

    getStatusText(status) {
        switch (status) {
            case 'connected': return 'Connected';
            case 'disconnected': return 'Disconnected';
            case 'warning': return 'Warning';
            default: return 'Unknown';
        }
    }

    updateLogsTable(data) {
        const tbody = document.getElementById('logsTableBody');
        
        if (!data.logs || data.logs.length === 0) {
            tbody.innerHTML = '<tr><td colspan="5" class="no-data">No logs found</td></tr>';
            return;
        }
        
        tbody.innerHTML = data.logs.map(log => `
            <tr>
                <td>${this.formatTimestamp(log.timestamp)}</td>
                <td><span class="status-badge ${log.stage}">${log.stage}</span></td>
                <td>${log.action}</td>
                <td><span class="status-badge ${log.status}">${log.status}</span></td>
                <td>${log.message || '-'}</td>
            </tr>
        `).join('');
        
        // Update pagination
        this.updatePagination(data.total, data.page, data.total_pages);
    }

    updatePagination(total, page, totalPages) {
        document.getElementById('pageInfo').textContent = `Page ${page} of ${totalPages}`;
        document.getElementById('prevPage').disabled = page <= 1;
        document.getElementById('nextPage').disabled = page >= totalPages;
    }



    async executeControl(action) {
        try {
            const response = await fetch(`/api/control/${action}`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                }
            });
            
            if (!response.ok) {
                throw new Error(`Failed to execute ${action}`);
            }
            
            const result = await response.json();
            this.showSuccess(`${action} executed successfully`);
            
            // Refresh data after control action
            setTimeout(() => this.refreshAllData(), 1000);
        } catch (error) {
            console.error(`Error executing ${action}:`, error);
            this.showError(`Failed to execute ${action}: ${error.message}`);
        }
    }

    addLogEntry(log) {
        // Add new log entry to the top of the table if we're on the first page
        if (this.currentPage === 1) {
            const tbody = document.getElementById('logsTableBody');
            const newRow = document.createElement('tr');
            newRow.innerHTML = `
                <td>${this.formatTimestamp(log.timestamp)}</td>
                <td><span class="status-badge ${log.stage}">${log.stage}</span></td>
                <td>${log.action}</td>
                <td><span class="status-badge ${log.status}">${log.status}</span></td>
                <td>${log.message || '-'}</td>
            `;
            
            tbody.insertBefore(newRow, tbody.firstChild);
            
            // Remove last row if we exceed the limit
            if (tbody.children.length > this.logsPerPage) {
                tbody.removeChild(tbody.lastChild);
            }
        }
    }

    startAutoRefresh() {
        this.refreshInterval = setInterval(() => {
            this.refreshAllData();
        }, 5000); // Refresh every 5 seconds
    }

    async refreshAllData() {
        try {
            await Promise.all([
                this.loadMetrics(),
                this.loadHealthStatus()
            ]);
        } catch (error) {
            console.error('Error refreshing data:', error);
        }
    }

    // Utility functions
    updateElement(id, value) {
        const element = document.getElementById(id);
        if (element) {
            element.textContent = value;
        }
    }

    formatNumber(num, decimals = 0) {
        if (num === null || num === undefined) return '-';
        return Number(num).toLocaleString(undefined, {
            minimumFractionDigits: decimals,
            maximumFractionDigits: decimals
        });
    }

    formatTimestamp(timestamp) {
        if (!timestamp) return '-';
        try {
            const date = new Date(timestamp);
            return date.toLocaleString();
        } catch (error) {
            return '-';
        }
    }

    debounce(func, wait) {
        clearTimeout(this.debounceTimer);
        this.debounceTimer = setTimeout(func, wait);
    }

    showLoadingOverlay() {
        document.getElementById('loadingOverlay').style.display = 'flex';
    }

    hideLoadingOverlay() {
        document.getElementById('loadingOverlay').style.display = 'none';
    }

    showError(message) {
        document.getElementById('errorMessage').textContent = message;
        document.getElementById('errorModal').classList.add('show');
    }

    showSuccess(message) {
        // You could implement a success toast here
        console.log('Success:', message);
    }

    hideModal() {
        document.querySelectorAll('.modal').forEach(modal => {
            modal.classList.remove('show');
        });
    }

    destroy() {
        if (this.autoRefreshInterval) {
            clearInterval(this.autoRefreshInterval);
        }
        
        if (this.websocket) {
            this.websocket.close();
        }
    }
}

// Initialize dashboard when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.dashboard = new CloudSyncDashboard();
});

// Cleanup on page unload
window.addEventListener('beforeunload', () => {
    if (window.dashboard) {
        window.dashboard.destroy();
    }
});