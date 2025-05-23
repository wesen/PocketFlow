class PocketFlowUI {
    constructor() {
        this.ws = null;
        this.autoScroll = true;
        this.activeFlows = new Map();
        this.init();
    }

    init() {
        this.loadFlows();
        this.setupEventListeners();
        this.connectWebSocket();
    }

    async loadFlows() {
        try {
            const response = await fetch('/api/flows');
            const flows = await response.json();
            const select = document.getElementById('flow-select');
            
            flows.forEach(flow => {
                const option = document.createElement('option');
                option.value = flow.id;
                option.textContent = flow.name;
                option.dataset.description = flow.description;
                select.appendChild(option);
            });
        } catch (error) {
            console.error('Failed to load flows:', error);
        }
    }

    setupEventListeners() {
        // Flow selection
        document.getElementById('flow-select').addEventListener('change', (e) => {
            const description = e.target.selectedOptions[0]?.dataset.description || '';
            document.getElementById('flow-description').textContent = description;
        });

        // Flow form submission
        document.getElementById('flow-form').addEventListener('submit', (e) => {
            e.preventDefault();
            this.startFlow();
        });

        // Clear log button
        document.getElementById('clear-log-btn').addEventListener('click', () => {
            document.getElementById('event-log').innerHTML = '';
        });

        // Auto-scroll toggle
        document.getElementById('auto-scroll-btn').addEventListener('click', (e) => {
            this.autoScroll = !this.autoScroll;
            e.target.textContent = this.autoScroll ? 'Auto-scroll: ON' : 'Auto-scroll: OFF';
            e.target.dataset.enabled = this.autoScroll;
        });
    }

    connectWebSocket() {
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = protocol + '//' + window.location.host + '/ws';
        
        this.ws = new WebSocket(wsUrl);
        
        this.ws.onopen = () => {
            this.updateConnectionStatus(true);
            this.addEventToLog('system', 'Connected to PocketFlow event stream', new Date());
        };
        
        this.ws.onclose = () => {
            this.updateConnectionStatus(false);
            this.addEventToLog('system', 'Disconnected from event stream', new Date());
            
            // Reconnect after 3 seconds
            setTimeout(() => this.connectWebSocket(), 3000);
        };
        
        this.ws.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);
                this.handleEvent(data);
            } catch (error) {
                console.error('Failed to parse WebSocket message:', error);
            }
        };
        
        this.ws.onerror = (error) => {
            console.error('WebSocket error:', error);
        };
    }

    updateConnectionStatus(connected) {
        const indicator = document.getElementById('connection-status');
        const text = document.getElementById('connection-text');
        
        if (connected) {
            indicator.className = 'status-indicator status-running';
            text.textContent = 'Connected';
        } else {
            indicator.className = 'status-indicator status-disconnected';
            text.textContent = 'Disconnected';
        }
    }

    async startFlow() {
        const flowType = document.getElementById('flow-select').value;
        const initialDataText = document.getElementById('initial-data').value.trim();
        
        if (!flowType) {
            this.showFlowResult('Please select a flow type', 'danger');
            return;
        }
        
        let initialData = {};
        if (initialDataText) {
            try {
                initialData = JSON.parse(initialDataText);
            } catch (error) {
                this.showFlowResult('Invalid JSON in initial data', 'danger');
                return;
            }
        }
        
        // Add timestamp
        initialData.started_at = new Date().toISOString();
        
        try {
            const response = await fetch('/api/flows/start', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    flow_type: flowType,
                    initial_data: initialData
                })
            });
            
            const result = await response.json();
            
            if (result.success) {
                this.showFlowResult('✅ Flow started successfully! ID: ' + result.flow_execution_id, 'success');
                this.activeFlows.set(result.flow_execution_id, {
                    type: flowType,
                    status: 'running',
                    startTime: new Date()
                });
                this.updateActiveFlowsDisplay();
            } else {
                this.showFlowResult('❌ Failed to start flow: ' + result.error, 'danger');
            }
        } catch (error) {
            this.showFlowResult('⚠️ Error starting flow: ' + error.message, 'danger');
        }
    }

    showFlowResult(message, type) {
        const alert = document.getElementById('flow-result-alert');
        const container = document.getElementById('flow-result');
        
        alert.className = 'alert alert-' + type;
        alert.textContent = message;
        container.style.display = 'block';
        
        // Auto-hide after 5 seconds for success messages
        if (type === 'success') {
            setTimeout(() => {
                container.style.display = 'none';
            }, 5000);
        }
    }

    handleEvent(event) {
        this.addEventToLog(event.event_type, this.formatEventMessage(event), new Date(event.timestamp));
        
        // Update active flows status
        if (event.flow_execution_id) {
            const flow = this.activeFlows.get(event.flow_execution_id);
            if (flow) {
                if (event.event_type === 'flow.completed') {
                    flow.status = 'completed';
                } else if (event.event_type === 'flow.failed') {
                    flow.status = 'failed';
                }
                this.updateActiveFlowsDisplay();
            }
        }
    }

    formatEventMessage(event) {
        const flowId = event.flow_execution_id ? event.flow_execution_id.substring(0, 8) : '';
        const nodeId = event.node_execution_id ? event.node_execution_id.substring(0, 8) : '';
        
        switch (event.event_type) {
            case 'flow.start.requested':
                return '🚀 Flow \'' + event.flow_type + '\' started [' + flowId + ']';
            case 'flow.completed':
                return '✅ Flow \'' + event.flow_type + '\' completed in ' + (event.duration || 'unknown') + 'ms [' + flowId + ']';
            case 'flow.failed':
                return '❌ Flow \'' + event.flow_type + '\' failed: ' + event.error_message + ' [' + flowId + ']';
            case 'node.exec.requested':
                return '⚙️ Node \'' + event.node_type + '\' (' + event.node_id + ') execution started [' + nodeId + ']';
            case 'node.completed':
                return '✓ Node \'' + event.node_type + '\' completed with action \'' + event.action + '\' in ' + (event.duration || 'unknown') + 'ms [' + nodeId + ']';
            case 'node.exec.failed':
                return '⚠️ Node \'' + event.node_type + '\' failed: ' + event.error_message + ' [' + nodeId + ']';
            case 'progress.update':
                return '📊 Progress: ' + event.message + ' (' + Math.round((event.progress || 0) * 100) + '%)';
            default:
                return '📡 ' + event.event_type + ': ' + JSON.stringify(event).substring(0, 100) + '...';
        }
    }

    addEventToLog(eventType, message, timestamp) {
        const log = document.getElementById('event-log');
        const eventDiv = document.createElement('div');
        eventDiv.className = 'event-item event-' + eventType.replace(/\./g, '-').replace(/_/g, '-');
        
        const timeStr = timestamp.toLocaleTimeString();
        eventDiv.innerHTML = 
            '<span class="timestamp">' + timeStr + '</span> ' +
            '<span class="event-type">' + eventType + '</span> ' +
            message;
        
        log.appendChild(eventDiv);
        
        // Auto-scroll to bottom if enabled
        if (this.autoScroll) {
            log.scrollTop = log.scrollHeight;
        }
        
        // Keep only last 100 events to prevent memory issues
        while (log.children.length > 100) {
            log.removeChild(log.firstChild);
        }
    }

    updateActiveFlowsDisplay() {
        const container = document.getElementById('active-flows');
        
        if (this.activeFlows.size === 0) {
            container.innerHTML = '<p class="text-muted">No active flows</p>';
            return;
        }
        
        let html = '';
        this.activeFlows.forEach((flow, id) => {
            const statusClass = 'status-' + flow.status;
            const shortId = id.substring(0, 8);
            const elapsed = Math.round((Date.now() - flow.startTime.getTime()) / 1000);
            
            html += 
                '<div class="d-flex align-items-center mb-2">' +
                '<span class="status-indicator ' + statusClass + '"></span>' +
                '<div class="flex-grow-1">' +
                '<strong>' + flow.type + '</strong>' +
                '<br>' +
                '<small class="text-muted">ID: ' + shortId + ' • ' + elapsed + 's ago</small>' +
                '</div>' +
                '</div>';
        });
        
        container.innerHTML = html;
    }
}

// Initialize the UI when the page loads
document.addEventListener('DOMContentLoaded', () => {
    new PocketFlowUI();
});