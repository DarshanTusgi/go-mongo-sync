// 🚀 GOD MODE Dashboard JavaScript - Enhanced Real-time Monitoring
class CloudSyncDashboard {
    constructor() {
        const path = window.location.pathname;
        const dashboardIndex = path.indexOf('/dashboard');
        this.basePath = dashboardIndex > 0 ? path.substring(0, dashboardIndex) : '';
        console.log('Detected base path:', this.basePath);
        this.websocket = null;
        this.refreshInterval = null;
        this.currentPage = 1;
        this.logsPerPage = 50;
        this.filters = {
            stage: '',
            status: '',
            search: ''
        };
        this.retryCount = 0;
        this.maxRetries = 3;
        this.connectionState = 'disconnected';
        
        this.init();
    }

    init() {
        console.log('🚀 GOD MODE Dashboard initializing...');
        this.showInitialData(); // Show placeholder data immediately
        this.hideLoadingOverlay();
        this.setupEventListeners();
        this.startAutoRefresh();
        this.connectWebSocket();
        this.loadInitialData();
        this.updateConnectionIndicator();
    }
    
    showInitialData() {
        console.log('📋 Setting up initial placeholder data...');
        // Set initial placeholder values to show the dashboard is working
        this.updateElement('totalDocs', 'Loading...');
        this.updateElement('todayDocs', 'Loading...');
        this.updateElement('syncRate', 'Loading...');
        this.updateElement('backlogSize', 'Loading...');
        this.updateElement('avgLatency', 'Loading...');
        this.updateElement('activeWatchers', 'Loading...');
        this.updateElement('lastResumeToken', 'Loading...');
        this.updateElement('syncMode', 'Loading...');
        this.updateElement('lastCheckpoint', 'Loading...');
        this.updateElement('connectedClients', 'Loading...');
        
        // Show initial status as checking
        ['sourceMongo', 'cloudSync', 'vmSync', 'targetMongo'].forEach(component => {
            this.updateConnectionStatus(component, 'warning');
        });
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
        const wsUrl = `${protocol}//${window.location.host}${this.basePath}/ws`;
        
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
        console.log('🚀 Loading initial dashboard data...');
        this.showLoadingOverlay();
        
        try {
            console.log('🔄 Starting parallel data loading...');
            await Promise.all([
                this.loadMetrics(),
                this.loadHealthStatus(),
                this.loadLogs()
            ]);
            console.log('✅ All initial data loaded successfully');
        } catch (error) {
            console.error('❌ Error loading initial data:', error);
            this.showError('Failed to load dashboard data: ' + error.message);
        } finally {
            this.hideLoadingOverlay();
        }
    }

    async loadMetrics() {
        try {
            console.log(`📊 Loading metrics from ${this.basePath}/api/dashboard/metrics...`);
            const response = await fetch(`${this.basePath}/api/dashboard/metrics`);
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            const data = await response.json();
            console.log('✅ Metrics loaded successfully:', data);
            this.updateMetrics(data);
            this.retryCount = 0; // Reset retry count on success
        } catch (error) {
            console.error('❌ Error loading metrics:', error);
            this.handleAPIError('metrics', error);
        }
    }

    async loadHealthStatus() {
        try {
            console.log(`🏥 Loading health status from ${this.basePath}/health...`);
            const response = await fetch(`${this.basePath}/health`);
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            const data = await response.json();
            console.log('✅ Health status loaded:', data);
            this.updateHealthStatus(data);
            this.retryCount = 0; // Reset retry count on success
        } catch (error) {
            console.error('❌ Error loading health status:', error);
            this.handleAPIError('health', error);
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
            
            console.log(`📜 Loading logs from ${this.basePath}/api/dashboard/logs?${params}...`);
            const response = await fetch(`${this.basePath}/api/dashboard/logs?${params}`);
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            const data = await response.json();
            console.log(`✅ Logs loaded: ${data.logs ? data.logs.length : 0} entries`, data);
            this.updateLogsTable(data);
            this.retryCount = 0; // Reset retry count on success
        } catch (error) {
            console.error('❌ Error loading logs:', error);
            this.handleAPIError('logs', error);
        }
    }



    updateMetrics(metrics) {
        console.log('📈 Updating dashboard metrics...', metrics);
        
        // Check if we have dashboard_metrics from the new API structure
        const dashboardMetrics = metrics.dashboard_metrics || metrics;
        const systemMetrics = metrics.system_metrics || {};
        const syncStatus = metrics.sync_status || {};
        
        console.log('Dashboard metrics extracted:', dashboardMetrics);
        console.log('System metrics extracted:', systemMetrics);
        
        // Update statistics cards using dashboard_metrics
        this.updateElementWithLog('totalDocs', this.formatNumber(dashboardMetrics.total_documents || 0));
        this.updateElementWithLog('todayDocs', this.formatNumber(dashboardMetrics.total_documents || 0)); // Use same as total for now
        this.updateElementWithLog('syncRate', this.formatNumber(dashboardMetrics.sync_rate || 0, 1));
        this.updateElementWithLog('backlogSize', this.formatNumber(dashboardMetrics.backlog_size || 0));
        this.updateElementWithLog('avgLatency', this.formatNumber(dashboardMetrics.avg_latency || 0, 1));
        this.updateElementWithLog('activeWatchers', dashboardMetrics.active_watchers || 0);
        
        // Update config info (fallback to original metrics structure)
        this.updateElementWithLog('lastResumeToken', this.formatTimestamp(metrics.last_resume_token || syncStatus.last_resume_token));
        this.updateElementWithLog('syncMode', metrics.sync_mode || syncStatus.sync_mode || 'Unknown');
        this.updateElementWithLog('lastCheckpoint', this.formatTimestamp(metrics.last_checkpoint || syncStatus.last_checkpoint));
        this.updateElementWithLog('connectedClients', metrics.connected_clients || systemMetrics.connected_clients || 0);
        
        console.log('✅ Dashboard metrics updated successfully');
    }

    updateHealthStatus(health) {
        console.log('🏥 Updating health status...', health);
        
        this.updateConnectionStatus('sourceMongo', health.source_mongo || 'unknown');
        this.updateConnectionStatus('cloudSync', health.cloud_sync || 'unknown');
        this.updateConnectionStatus('vmSync', health.vm_sync || 'unknown');
        this.updateConnectionStatus('targetMongo', health.target_mongo || 'unknown');
        
        // Update VM sync details if available
        if (health.vm_sync_info) {
            console.log('📊 Updating VM sync details:', health.vm_sync_info);
            this.updateVMSyncDetails(health.vm_sync_info);
        }
        
        console.log('✅ Health status updated successfully');
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
            const targetDatabases = vmSyncInfo.target_databases || 0;
            clientsElement.innerHTML = `
                <div>Connected clients: ${clientCount}</div>
                <div>Target databases: ${targetDatabases}</div>
                <div>Transport: ${vmSyncInfo.transport_mode || 'HTTP'}</div>
            `;
            
            // Show detailed client information if available
            if (vmSyncInfo.client_details && vmSyncInfo.client_details.length > 0) {
                const detailsHtml = vmSyncInfo.client_details.map(client => 
                    `<div class="client-detail" style="margin-top: 4px; font-size: 0.8em; color: #888;">` +
                    `${client.client_id} (${client.status}) - Connected: ${new Date(client.connected_at).toLocaleTimeString()}</div>`
                ).join('');
                clientsElement.innerHTML += detailsHtml;
            }
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
        console.log('📜 Updating logs table...', data);
        const tbody = document.getElementById('logsTableBody');
        
        if (!tbody) {
            console.error('❌ Logs table body element not found');
            return;
        }
        
        if (!data.logs || data.logs.length === 0) {
            console.log('⚠️ No logs found, showing empty state');
            tbody.innerHTML = '<tr><td colspan="5" class="no-data">No logs found</td></tr>';
            return;
        }
        
        console.log(`📄 Rendering ${data.logs.length} log entries`);
        tbody.innerHTML = data.logs.map((log, index) => {
            console.log(`Log entry ${index}:`, log);
            return `
                <tr>
                    <td>${this.formatTimestamp(log.timestamp)}</td>
                    <td><span class="status-badge ${log.stage}">${log.stage}</span></td>
                    <td>${log.action}</td>
                    <td><span class="status-badge ${log.status}">${log.status}</span></td>
                    <td>${log.message || '-'}</td>
                </tr>
            `;
        }).join('');
        
        // Update pagination
        this.updatePagination(data.total, data.page, data.total_pages);
        console.log('✅ Logs table updated successfully');
    }

    updatePagination(total, page, totalPages) {
        document.getElementById('pageInfo').textContent = `Page ${page} of ${totalPages}`;
        document.getElementById('prevPage').disabled = page <= 1;
        document.getElementById('nextPage').disabled = page >= totalPages;
    }



    async executeControl(action) {
        try {
            const response = await fetch(`${this.basePath}/api/control/${action}`, {
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
        } else {
            console.warn(`⚠️ Element with ID '${id}' not found in DOM`);
        }
    }
    
    updateElementWithLog(id, value) {
        console.log(`📋 Updating element '${id}' with value:`, value);
        const element = document.getElementById(id);
        if (element) {
            const oldValue = element.textContent;
            element.textContent = value;
            console.log(`✅ Updated '${id}': '${oldValue}' -> '${value}'`);
        } else {
            console.error(`❌ Element with ID '${id}' not found in DOM`);
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
    
    // 🚀 GOD MODE: Enhanced error handling and monitoring
    handleAPIError(endpoint, error) {
        console.error(`❌ API Error [${endpoint}]:`, error);
        this.retryCount++;
        
        if (this.retryCount < this.maxRetries) {
            console.log(`🔄 Retrying ${endpoint} (${this.retryCount}/${this.maxRetries})...`);
            setTimeout(() => {
                if (endpoint === 'metrics') this.loadMetrics();
                else if (endpoint === 'health') this.loadHealthStatus();
                else if (endpoint === 'logs') this.loadLogs();
            }, 2000 * this.retryCount); // Exponential backoff
        } else {
            this.showError(`Failed to load ${endpoint} after ${this.maxRetries} attempts`);
        }
    }
    
    updateConnectionIndicator() {
        const indicator = document.querySelector('.refresh-dot');
        if (indicator) {
            indicator.style.background = this.connectionState === 'connected' ? '#22c55e' : '#ef4444';
        }
    }
    
    // Enhanced logging for GOD MODE debugging
    logDashboardEvent(event, data = {}) {
        console.log(`🎯 Dashboard Event [${event}]:`, {
            timestamp: new Date().toISOString(),
            connectionState: this.connectionState,
            retryCount: this.retryCount,
            ...data
        });
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