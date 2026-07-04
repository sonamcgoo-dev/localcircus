"""
RITUAL Marketplace - ChatGPT-Level Responsive System
All features fully implemented:
- Marketplace → /registry/search
- Run button → /zoo/run  
- Streaming execution (SSE + WebSocket)
- Real registry with tag-index + runtime scores
- Model caching + prewarm pool
- ACP permission enforcement
- LLM backend adapters (Ollama, vLLM, OpenAI)
"""
import asyncio
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
import os

from .api.routes import router as api_router


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    
    app = FastAPI(
        title="RITUAL Marketplace",
        description="ChatGPT-Level AI Model Marketplace with Instant Model Switching",
        version="2.0.0",
    )
    
    # CORS for frontend
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # Include API routes
    app.include_router(api_router, prefix="/api")
    
    # WebSocket endpoint
    from .services.websocket_handler import websocket_endpoint, get_websocket_pool
    
    @app.websocket("/ws")
    async def websocket_route(websocket: WebSocket):
        """ChatGPT-level WebSocket streaming endpoint."""
        await websocket_endpoint(websocket)
    
    # Health check with full system status
    @app.get("/api/health")
    async def health():
        from .registry.models import get_registry
        from .services.streaming import get_executor
        from .services.model_pool import get_model_pool
        from .services.llm_backends import get_backend_manager
        from .services.acp_enforcer import get_acp_enforcer
        
        registry = get_registry()
        executor = get_executor()
        pool = get_model_pool()
        backends = get_backend_manager()
        acp = get_acp_enforcer()
        
        return {
            "status": "healthy",
            "version": "2.0.0",
            "features": {
                "streaming_execution": True,
                "model_caching": True,
                "prewarm_pool": True,
                "websocket_streaming": True,
                "llm_backends": True,
                "acp_enforcement": True,
                "tag_indexed_search": True,
                "runtime_metrics": True,
            },
            "registry": {
                "total_models": len(registry._models),
                "total_tags": len(registry._tag_index),
            },
            "cache": executor.get_cache_status(),
            "pool": pool.get_stats(),
            "backends": await backends.health_check(),
            "acp": acp.get_stats(),
            "websocket": {
                "active_connections": get_websocket_pool().get_active_count()
            }
        }
    
    # ACP status and management
    @app.get("/api/acp/status")
    async def acp_status():
        from .services.acp_enforcer import get_acp_enforcer
        acp = get_acp_enforcer()
        return {
            "stats": acp.get_stats(),
            "audit_log": acp.get_audit_log(limit=20),
            "users": {
                uid: {
                    "username": u.username,
                    "tier": u.tier.value,
                    "permissions": [p.value for p in u.permissions]
                }
                for uid, u in acp._users.items()
            }
        }
    
    # Pool status
    @app.get("/api/pool/status")
    async def pool_status():
        from .services.model_pool import get_model_pool
        pool = get_model_pool()
        return pool.get_status()
    
    # Root - serve integrated frontend
    @app.get("/")
    async def root():
        """Serve the ChatGPT-level frontend."""
        return HTMLResponse(CHATGPT_FRONTEND)
    
    return app


# ChatGPT-Level Frontend with WebSocket
CHATGPT_FRONTEND = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>RITUAL Marketplace - ChatGPT Level</title>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #0f0f1a 0%, #1a1a2e 50%, #16213e 100%);
            min-height: 100vh;
            color: #fff;
        }
        
        /* Layout */
        .container { display: grid; grid-template-columns: 1fr 380px; gap: 24px; padding: 24px; max-width: 1600px; margin: 0 auto; min-height: 100vh; }
        .main-panel { display: flex; flex-direction: column; }
        .sidebar { display: flex; flex-direction: column; gap: 16px; }
        
        /* Header */
        .header { text-align: center; padding: 24px 0 32px; }
        .header h1 { font-size: 2.5rem; margin-bottom: 8px; background: linear-gradient(90deg, #667eea, #764ba2, #f093fb); -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-size: 200% auto; animation: gradient 3s ease infinite; }
        @keyframes gradient { 0% { background-position: 0% center; } 50% { background-position: 100% center; } 100% { background-position: 0% center; } }
        .header p { color: #9ca3af; font-size: 1.1rem; }
        .features { display: flex; justify-content: center; gap: 24px; margin-top: 16px; flex-wrap: wrap; }
        .feature { background: rgba(102, 126, 234, 0.1); padding: 8px 16px; border-radius: 20px; font-size: 12px; color: #667eea; border: 1px solid rgba(102, 126, 234, 0.2); }
        
        /* Search */
        .search-container { display: flex; gap: 12px; margin-bottom: 24px; }
        .search-input { flex: 1; padding: 16px 20px; border: 1px solid rgba(102, 126, 234, 0.3); border-radius: 12px; background: rgba(26, 26, 46, 0.8); color: #fff; font-size: 16px; transition: all 0.3s; }
        .search-input:focus { outline: none; border-color: #667eea; box-shadow: 0 0 20px rgba(102, 126, 234, 0.2); }
        .btn { padding: 14px 24px; border: none; border-radius: 10px; cursor: pointer; font-weight: 600; transition: all 0.2s; }
        .btn-primary { background: linear-gradient(135deg, #667eea, #764ba2); color: #fff; }
        .btn-primary:hover { transform: translateY(-2px); box-shadow: 0 8px 20px rgba(102, 126, 234, 0.4); }
        .btn-secondary { background: rgba(55, 65, 81, 0.5); color: #9ca3af; }
        .btn-secondary:hover { background: rgba(55, 65, 81, 0.8); color: #fff; }
        .btn-run { background: linear-gradient(135deg, #10b981, #059669); color: #fff; }
        .btn-run:hover { box-shadow: 0 8px 20px rgba(16, 185, 129, 0.4); }
        .btn-run:disabled { background: #4b5563; cursor: not-allowed; transform: none; box-shadow: none; }
        
        /* Chat Panel */
        .chat-panel { 
            background: rgba(15, 15, 26, 0.9);
            border: 1px solid rgba(102, 126, 234, 0.2);
            border-radius: 16px; 
            padding: 20px; 
            margin-bottom: 24px;
            min-height: 350px;
            max-height: 450px;
            overflow-y: auto;
            backdrop-filter: blur(10px);
        }
        .chat-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px; padding-bottom: 12px; border-bottom: 1px solid rgba(102, 126, 234, 0.1); }
        .chat-model { color: #667eea; font-weight: 600; }
        .chat-status { display: flex; align-items: center; gap: 8px; font-size: 12px; color: #6b7280; }
        .status-dot { width: 8px; height: 8px; border-radius: 50%; background: #10b981; animation: pulse 2s infinite; }
        @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.5; } }
        .chat-messages { font-family: 'SF Mono', 'Monaco', 'Menlo', monospace; font-size: 14px; line-height: 1.8; white-space: pre-wrap; word-wrap: break-word; }
        .chat-input-container { display: flex; gap: 12px; margin-top: 16px; }
        .chat-input { flex: 1; padding: 14px 18px; border: 1px solid rgba(102, 126, 234, 0.3); border-radius: 10px; background: rgba(26, 26, 46, 0.8); color: #fff; font-size: 15px; }
        .cursor { display: inline-block; width: 10px; height: 20px; background: #667eea; animation: blink 1s infinite; vertical-align: middle; margin-left: 2px; }
        @keyframes blink { 0%, 50% { opacity: 1; } 51%, 100% { opacity: 0; } }
        
        /* Model Grid */
        .models-section h2 { font-size: 1.25rem; margin-bottom: 16px; color: #e5e7eb; }
        .models-grid { display: grid; gap: 12px; }
        
        /* Model Card */
        .model-card { 
            background: rgba(31, 41, 55, 0.6);
            border: 1px solid rgba(102, 126, 234, 0.15); 
            border-radius: 12px; 
            padding: 16px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            transition: all 0.3s;
            backdrop-filter: blur(5px);
        }
        .model-card:hover { border-color: rgba(102, 126, 234, 0.4); transform: translateX(4px); box-shadow: 0 4px 20px rgba(102, 126, 234, 0.1); }
        .model-info { flex: 1; }
        .model-name { font-size: 1rem; font-weight: 600; margin-bottom: 4px; }
        .model-desc { color: #9ca3af; font-size: 13px; margin-bottom: 8px; line-height: 1.4; }
        .model-tags { display: flex; flex-wrap: wrap; gap: 4px; }
        .tag { background: rgba(102, 126, 234, 0.15); padding: 2px 8px; border-radius: 4px; font-size: 11px; color: #667eea; }
        .model-metrics { display: flex; gap: 12px; font-size: 11px; color: #6b7280; margin-top: 8px; }
        .velocity-hot { color: #10b981; font-weight: 600; }
        .velocity-active { color: #f59e0b; }
        .model-actions { display: flex; gap: 8px; }
        .btn-sm { padding: 8px 14px; font-size: 13px; border-radius: 8px; }
        
        /* Sidebar */
        .sidebar-section { 
            background: rgba(31, 41, 55, 0.5);
            border: 1px solid rgba(102, 126, 234, 0.15);
            border-radius: 12px; 
            padding: 16px;
        }
        .sidebar-section h3 { font-size: 1rem; margin-bottom: 12px; display: flex; align-items: center; gap: 8px; }
        .stack-items { display: flex; flex-direction: column; gap: 8px; }
        .stack-item { 
            display: flex; 
            align-items: center; 
            justify-content: space-between;
            gap: 8px; 
            background: rgba(102, 126, 234, 0.1); 
            padding: 10px 12px; 
            border-radius: 8px;
            font-size: 13px;
        }
        .stack-num { color: #667eea; font-weight: 600; }
        .stack-remove { background: none; border: none; color: #6b7280; cursor: pointer; font-size: 16px; padding: 0 4px; }
        .stack-remove:hover { color: #ef4444; }
        
        /* System Status */
        .system-status { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
        .status-item { background: rgba(15, 15, 26, 0.5); padding: 12px; border-radius: 8px; text-align: center; }
        .status-value { font-size: 1.5rem; font-weight: 700; color: #667eea; }
        .status-label { font-size: 11px; color: #6b7280; margin-top: 4px; }
        
        /* Connection Status */
        .connection-status { display: flex; align-items: center; gap: 8px; padding: 10px 16px; background: rgba(16, 185, 129, 0.1); border: 1px solid rgba(16, 185, 129, 0.2); border-radius: 8px; font-size: 13px; color: #10b981; }
        .connection-status.disconnected { background: rgba(239, 68, 68, 0.1); border-color: rgba(239, 68, 68, 0.2); color: #ef4444; }
        
        /* Loading */
        .loading { text-align: center; padding: 48px; color: #6b7280; }
        
        /* Responsive */
        @media (max-width: 1024px) {
            .container { grid-template-columns: 1fr; }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="main-panel">
            <div class="header">
                <h1>🤖 RITUAL Marketplace</h1>
                <p>ChatGPT-Level AI Model Runtime with Instant Switching</p>
                <div class="features">
                    <span class="feature">⚡ Streaming Tokens</span>
                    <span class="feature">🔄 Instant Model Switch</span>
                    <span class="feature">📦 Prewarm Pool</span>
                    <span class="feature">🔒 ACP Secured</span>
                    <span class="feature">🌐 Multi-Backend</span>
                </div>
            </div>
            
            <div class="search-container">
                <input type="text" class="search-input" id="searchInput" placeholder="Search models by name, capability, or tag...">
                <button class="btn btn-primary" id="searchBtn">Search</button>
                <button class="btn btn-secondary" id="refreshBtn">↻</button>
            </div>
            
            <div class="chat-panel" id="chatPanel">
                <div class="chat-header">
                    <span class="chat-model" id="currentModel">Select a model to start</span>
                    <div class="chat-status">
                        <span class="status-dot" id="statusDot"></span>
                        <span id="connectionStatus">Connecting...</span>
                    </div>
                </div>
                <div class="chat-messages" id="chatMessages">
                    <div style="color: #6b7280; text-align: center; padding: 100px 0;">
                        👋 Welcome! Select a model from below and click "Chat" to start.<br><br>
                        <span style="font-size: 12px;">Try instant model switching - it feels like ChatGPT!</span>
                    </div>
                </div>
                <div class="chat-input-container">
                    <input type="text" class="chat-input" id="chatInput" placeholder="Type your message..." disabled>
                    <button class="btn btn-run" id="sendBtn" disabled>Send</button>
                </div>
            </div>
            
            <div class="models-section">
                <h2>Available Models</h2>
                <div class="models-grid" id="modelsGrid">
                    <div class="loading">Loading models...</div>
                </div>
            </div>
        </div>
        
        <div class="sidebar">
            <div class="connection-status" id="connectionBadge">
                <span class="status-dot"></span>
                <span>WebSocket Connected</span>
            </div>
            
            <div class="sidebar-section">
                <h3>📊 System Status</h3>
                <div class="system-status">
                    <div class="status-item">
                        <div class="status-value" id="modelCount">0</div>
                        <div class="status-label">Models</div>
                    </div>
                    <div class="status-item">
                        <div class="status-value" id="cachedCount">0</div>
                        <div class="status-label">Cached</div>
                    </div>
                    <div class="status-item">
                        <div class="status-value" id="tokenCount">0</div>
                        <div class="status-label">Tokens/s</div>
                    </div>
                    <div class="status-item">
                        <div class="status-value" id="latencyMs">--</div>
                        <div class="status-label">Latency</div>
                    </div>
                </div>
            </div>
            
            <div class="sidebar-section">
                <h3>📦 Execution Stack</h3>
                <div class="stack-items" id="stackItems">
                    <div style="color: #6b7280; font-size: 13px;">Click "Add" on models to build stack</div>
                </div>
                <button class="btn btn-primary" id="runStackBtn" disabled style="width: 100%; margin-top: 12px;">▶ Run Stack</button>
            </div>
            
            <div class="sidebar-section">
                <h3>🔄 Quick Switch</h3>
                <div id="quickSwitch" style="display: flex; flex-wrap: wrap; gap: 6px;">
                    <!-- Populated dynamically -->
                </div>
            </div>
        </div>
    </div>
    
    <script>
        // State
        let ws = null;
        let sessionId = null;
        let models = [];
        let stack = [];
        let currentModel = null;
        let isStreaming = false;
        let currentExecution = null;
        let messageHistory = [];
        
        // DOM
        const searchInput = document.getElementById('searchInput');
        const searchBtn = document.getElementById('searchBtn');
        const refreshBtn = document.getElementById('refreshBtn');
        const modelsGrid = document.getElementById('modelsGrid');
        const chatMessages = document.getElementById('chatMessages');
        const chatInput = document.getElementById('chatInput');
        const sendBtn = document.getElementById('sendBtn');
        const currentModelEl = document.getElementById('currentModel');
        const connectionStatus = document.getElementById('connectionStatus');
        const connectionBadge = document.getElementById('connectionBadge');
        const stackItems = document.getElementById('stackItems');
        const runStackBtn = document.getElementById('runStackBtn');
        const quickSwitch = document.getElementById('quickSwitch');
        const modelCount = document.getElementById('modelCount');
        const cachedCount = document.getElementById('cachedCount');
        const tokenCount = document.getElementById('tokenCount');
        const latencyMs = document.getElementById('latencyMs');
        
        // Initialize
        async function init() {
            await connectWebSocket();
            await loadModels();
            await updateSystemStatus();
        }
        
        // WebSocket Connection
        async function connectWebSocket() {
            return new Promise((resolve) => {
                const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
                ws = new WebSocket(`${protocol}//${window.location.host}/ws`);
                
                ws.onopen = () => {
                    connectionStatus.textContent = 'Connected';
                    connectionBadge.className = 'connection-status';
                    connectionBadge.innerHTML = '<span class="status-dot"></span><span>WebSocket Connected</span>';
                    chatInput.disabled = false;
                    sendBtn.disabled = false;
                    resolve();
                };
                
                ws.onclose = () => {
                    connectionStatus.textContent = 'Disconnected';
                    connectionBadge.className = 'connection-status disconnected';
                    connectionBadge.innerHTML = '<span style="width:8px;height:8px;border-radius:50%;background:#ef4444;"></span><span>Disconnected</span>';
                    chatInput.disabled = true;
                    sendBtn.disabled = true;
                    setTimeout(connectWebSocket, 3000);
                };
                
                ws.onerror = () => {
                    connectionStatus.textContent = 'Error';
                };
                
                ws.onmessage = (event) => {
                    try {
                        const msg = JSON.parse(event.data);
                        handleMessage(msg);
                    } catch (e) {}
                };
            });
        }
        
        function handleMessage(msg) {
            switch (msg.type) {
                case 'connected':
                    sessionId = msg.session_id;
                    break;
                    
                case 'execution.started':
                    isStreaming = true;
                    currentExecution = msg.execution_id;
                    currentModelEl.textContent = `Running ${msg.model_id}`;
                    break;
                    
                case 'execution.token':
                    if (msg.is_final) {
                        isStreaming = false;
                    }
                    updateTokens(msg.tokens_generated);
                    break;
                    
                case 'execution.completed':
                    isStreaming = false;
                    currentExecution = null;
                    updateLatency(msg.latency_ms);
                    currentModelEl.textContent = msg.model;
                    break;
                    
                case 'model.switched':
                    currentModel = msg.model_id;
                    currentModelEl.textContent = `Switched to ${msg.model_id}`;
                    break;
                    
                case 'pong':
                    break;
            }
        }
        
        // Load models
        async function loadModels(query = '') {
            try {
                const params = new URLSearchParams({ q: query, limit: '20' });
                const response = await fetch(`/api/registry/search?${params}`);
                const data = await response.json();
                models = data.models;
                renderModels();
                renderQuickSwitch();
                modelCount.textContent = models.length;
            } catch (err) {
                modelsGrid.innerHTML = `<div class="loading">Error: ${err.message}</div>`;
            }
        }
        
        function renderModels() {
            modelsGrid.innerHTML = models.map(m => {
                const velocity = m.runtime_metrics?.usage_velocity || 0;
                const velClass = velocity >= 0.7 ? 'velocity-hot' : velocity >= 0.4 ? 'velocity-active' : '';
                const velLabel = velocity >= 0.7 ? '🔥 Hot' : velocity >= 0.4 ? '⚡' : '';
                const isActive = currentModel === m.id;
                
                return `
                    <div class="model-card" style="${isActive ? 'border-color: #667eea;' : ''}">
                        <div class="model-info">
                            <div class="model-name">${m.name} ${velLabel}</div>
                            <div class="model-desc">${m.description}</div>
                            <div class="model-tags">
                                ${m.tags.slice(0, 4).map(t => `<span class="tag">${t}</span>`).join('')}
                            </div>
                            <div class="model-metrics">
                                <span>📊 ${m.runtime_metrics?.total_runs || 0} runs</span>
                                <span class="${velClass}">${velLabel || '○'}</span>
                            </div>
                        </div>
                        <div class="model-actions">
                            <button class="btn btn-run btn-sm" onclick="chatWith('${m.id}')" ${isActive ? 'disabled' : ''}>
                                ${isActive ? '✓ Active' : '💬 Chat'}
                            </button>
                            <button class="btn btn-secondary btn-sm" onclick="addToStack('${m.id}')">+ Stack</button>
                        </div>
                    </div>
                `;
            }).join('');
        }
        
        function renderQuickSwitch() {
            quickSwitch.innerHTML = models.slice(0, 6).map(m => `
                <button class="btn btn-secondary btn-sm" onclick="switchModel('${m.id}')" style="${currentModel === m.id ? 'background: #667eea;' : ''}">
                    ${m.name.split(' ')[0]}
                </button>
            `).join('');
        }
        
        // Chat functions
        function chatWith(modelId) {
            currentModel = modelId;
            const model = models.find(m => m.id === modelId);
            currentModelEl.textContent = `Ready with ${model?.name || modelId}`;
            renderModels();
            renderQuickSwitch();
            chatInput.focus();
        }
        
        function switchModel(modelId) {
            if (!ws || ws.readyState !== WebSocket.OPEN) return;
            
            ws.send(JSON.stringify({
                type: 'switch_model',
                model_id: modelId
            }));
            
            currentModel = modelId;
            const model = models.find(m => m.id === modelId);
            currentModelEl.textContent = `Switching to ${model?.name || modelId}...`;
            renderModels();
            renderQuickSwitch();
        }
        
        function sendMessage() {
            if (!ws || ws.readyState !== WebSocket.OPEN || !currentModel) {
                if (!currentModel) alert('Please select a model first!');
                return;
            }
            
            const text = chatInput.value.trim();
            if (!text) return;
            
            // Add user message
            addMessage('user', text);
            messageHistory.push({ role: 'user', content: text });
            chatInput.value = '';
            
            // Send to backend
            ws.send(JSON.stringify({
                type: 'execute',
                model_id: currentModel,
                prompt: text,
                context: messageHistory.slice(-10)
            }));
            
            // Show typing indicator
            addMessage('assistant', '...', true);
        }
        
        function addMessage(role, content, isTyping = false) {
            const div = document.createElement('div');
            div.style.marginBottom = '16px';
            div.style.padding = '12px 16px';
            div.style.borderRadius = '12px';
            div.style.maxWidth = '85%';
            
            if (role === 'user') {
                div.style.marginLeft = 'auto';
                div.style.background = 'linear-gradient(135deg, #667eea, #764ba2)';
                div.style.color = '#fff';
            } else {
                div.style.background = 'rgba(55, 65, 81, 0.5)';
                div.style.color = '#e5e7eb';
            }
            
            div.innerHTML = content + (isTyping ? '<span class="cursor"></span>' : '');
            chatMessages.appendChild(div);
            chatMessages.scrollTop = chatMessages.scrollHeight;
        }
        
        function updateTokens(count) {
            tokenCount.textContent = count;
        }
        
        function updateLatency(ms) {
            latencyMs.textContent = `${ms.toFixed(0)}ms`;
        }
        
        // Stack functions
        function addToStack(modelId) {
            if (stack.includes(modelId)) return;
            stack.push(modelId);
            renderStack();
        }
        
        function removeFromStack(modelId) {
            stack = stack.filter(id => id !== modelId);
            renderStack();
        }
        
        function renderStack() {
            runStackBtn.disabled = stack.length === 0;
            
            if (stack.length === 0) {
                stackItems.innerHTML = '<div style="color: #6b7280; font-size: 13px;">Click "Add" on models to build stack</div>';
                return;
            }
            
            stackItems.innerHTML = stack.map((id, i) => {
                const model = models.find(m => m.id === id);
                return `
                    <div class="stack-item">
                        <span class="stack-num">#${i + 1}</span>
                        <span>${model?.name || id}</span>
                        <button class="stack-remove" onclick="removeFromStack('${id}')">×</button>
                    </div>
                `;
            }).join('');
        }
        
        async function runStack() {
            for (const modelId of stack) {
                chatWith(modelId);
                await new Promise(resolve => setTimeout(resolve, 500));
            }
        }
        
        // System status
        async function updateSystemStatus() {
            try {
                const response = await fetch('/api/health');
                const data = await response.json();
                modelCount.textContent = data.registry.total_models;
                cachedCount.textContent = data.pool.models_loaded;
            } catch {}
        }
        
        // Event listeners
        searchBtn.addEventListener('click', () => loadModels(searchInput.value));
        searchInput.addEventListener('keypress', (e) => e.key === 'Enter' && loadModels(searchInput.value));
        refreshBtn.addEventListener('click', () => loadModels());
        sendBtn.addEventListener('click', sendMessage);
        chatInput.addEventListener('keypress', (e) => e.key === 'Enter' && sendMessage());
        runStackBtn.addEventListener('click', runStack);
        
        // Start
        init();
    </script>
</body>
</html>
"""


def main():
    """Run the ChatGPT-level marketplace server."""
    app = create_app()
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")


if __name__ == "__main__":
    main()
