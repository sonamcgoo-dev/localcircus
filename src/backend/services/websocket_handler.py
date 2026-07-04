"""
WebSocket Handler for bidirectional streaming.
Enables real-time token delivery with client-server communication.
"""
import asyncio
import json
import time
import uuid
from typing import Dict, Set, Optional, Any
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict
import weakref

from fastapi import WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState

from .llm_backends import get_backend_manager, LLMBackendManager
from .streaming import ModelCache


class ConnectionState(Enum):
    CONNECTING = "connecting"
    AUTHENTICATED = "authenticated"
    READY = "ready"
    EXECUTING = "executing"
    CLOSED = "closed"


@dataclass
class SessionContext:
    """WebSocket session context."""
    session_id: str
    user_id: Optional[str] = None
    permissions: Set[str] = field(default_factory=set)
    state: ConnectionState = ConnectionState.CONNECTING
    current_model: Optional[str] = None
    connected_at: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)


class WebSocketPool:
    """
    Manages WebSocket connections with proper cleanup.
    Enables broadcasting and targeted messaging.
    """
    
    def __init__(self):
        # Active connections by session ID
        self._connections: Dict[str, WebSocket] = {}
        # Session metadata
        self._sessions: Dict[str, SessionContext] = {}
        # Subscriptions by model (for broadcasting)
        self._model_subscriptions: Dict[str, Set[str]] = defaultdict(set)
        # Global broadcast subscribers
        self._broadcast_subscribers: Set[str] = set()
        # Lock for thread safety
        self._lock = asyncio.Lock()
    
    async def connect(self, websocket: WebSocket, session_id: str, user_id: str = None, permissions: Set[str] = None) -> SessionContext:
        """Accept and register a new WebSocket connection."""
        await websocket.accept()
        
        context = SessionContext(
            session_id=session_id,
            user_id=user_id,
            permissions=permissions or set()
        )
        
        async with self._lock:
            self._connections[session_id] = websocket
            self._sessions[session_id] = context
        
        await self._send_message(websocket, {
            "type": "connected",
            "session_id": session_id,
            "timestamp": time.time()
        })
        
        return context
    
    async def disconnect(self, session_id: str):
        """Clean up a disconnected session."""
        async with self._lock:
            if session_id in self._connections:
                del self._connections[session_id]
            if session_id in self._sessions:
                del self._sessions[session_id]
            # Remove from all subscriptions
            for subscribers in self._model_subscriptions.values():
                subscribers.discard(session_id)
            self._broadcast_subscribers.discard(session_id)
    
    async def send_to_session(self, session_id: str, message: dict) -> bool:
        """Send message to specific session."""
        async with self._lock:
            websocket = self._connections.get(session_id)
        
        if websocket:
            try:
                await websocket.send_json(message)
                return True
            except Exception:
                await self.disconnect(session_id)
                return False
        return False
    
    async def broadcast_to_model(self, model_id: str, message: dict):
        """Broadcast message to all sessions subscribed to a model."""
        session_ids = self._model_subscriptions.get(model_id, set()).copy()
        for session_id in session_ids:
            await self.send_to_session(session_id, message)
    
    async def broadcast_all(self, message: dict):
        """Broadcast message to all connected sessions."""
        session_ids = list(self._connections.keys())
        for session_id in session_ids:
            await self.send_to_session(session_id, message)
    
    def subscribe_to_model(self, session_id: str, model_id: str):
        """Subscribe session to model updates."""
        self._model_subscriptions[model_id].add(session_id)
    
    def unsubscribe_from_model(self, session_id: str, model_id: str):
        """Unsubscribe session from model updates."""
        self._model_subscriptions[model_id].discard(session_id)
    
    def get_session(self, session_id: str) -> Optional[SessionContext]:
        """Get session context."""
        return self._sessions.get(session_id)
    
    def get_active_count(self) -> int:
        """Get count of active connections."""
        return len(self._connections)
    
    async def _send_message(self, websocket: WebSocket, message: dict):
        """Send JSON message to websocket."""
        try:
            await websocket.send_json(message)
        except Exception:
            pass


class ChatGPTLevelSession:
    """
    ChatGPT-style streaming session.
    Handles the full lifecycle of a streaming conversation.
    """
    
    def __init__(
        self,
        pool: WebSocketPool,
        backend_manager: LLMBackendManager,
        model_cache: ModelCache,
        session_id: str,
        acp_enforcer=None
    ):
        self.pool = pool
        self.backend_manager = backend_manager
        self.model_cache = model_cache
        self.session_id = session_id
        self.acp_enforcer = acp_enforcer
        self._running_tasks: Dict[str, asyncio.Task] = {}
    
    async def handle_message(self, websocket: WebSocket, message: dict):
        """Handle incoming WebSocket message."""
        msg_type = message.get("type")
        
        handlers = {
            "execute": self._handle_execute,
            "cancel": self._handle_cancel,
            "subscribe": self._handle_subscribe,
            "unsubscribe": self._handle_unsubscribe,
            "ping": self._handle_ping,
            "prewarm": self._handle_prewarm,
            "switch_model": self._handle_switch_model,
        }
        
        handler = handlers.get(msg_type)
        if handler:
            await handler(websocket, message)
        else:
            await self.pool.send_to_session(self.session_id, {
                "type": "error",
                "error": f"Unknown message type: {msg_type}"
            })
    
    async def _handle_execute(self, websocket: WebSocket, message: dict):
        """Execute a model with streaming response."""
        model_id = message.get("model_id")
        prompt = message.get("prompt", "")
        execution_id = message.get("execution_id") or str(uuid.uuid4())
        temperature = message.get("temperature", 0.7)
        max_tokens = message.get("max_tokens", 2048)
        context = message.get("context", [])
        
        # ACP permission check
        if self.acp_enforcer:
            allowed, reason = await self.acp_enforcer.check_execution(self.session_id, model_id)
            if not allowed:
                await self.pool.send_to_session(self.session_id, {
                    "type": "execution.blocked",
                    "execution_id": execution_id,
                    "reason": reason
                })
                return
        
        # Update session state
        session = self.pool.get_session(self.session_id)
        if session:
            session.state = ConnectionState.EXECUTING
            session.current_model = model_id
        
        # Send execution started
        await self.pool.send_to_session(self.session_id, {
            "type": "execution.started",
            "execution_id": execution_id,
            "model_id": model_id,
            "timestamp": time.time()
        })
        
        # Check model cache
        cache_hit = self.model_cache.is_cached(model_id)
        if not cache_hit:
            await self.pool.send_to_session(self.session_id, {
                "type": "execution.status",
                "execution_id": execution_id,
                "status": "loading_model",
                "message": f"Loading {model_id}..."
            })
        
        # Get backend for this model
        backend = self.backend_manager.get_backend_for_model(model_id)
        
        # Run async execution task
        task = asyncio.create_task(
            self._execute_streaming(
                execution_id=execution_id,
                model_id=model_id,
                prompt=prompt,
                backend=backend,
                temperature=temperature,
                max_tokens=max_tokens,
                context=context
            )
        )
        self._running_tasks[execution_id] = task
        
        try:
            await task
        finally:
            self._running_tasks.pop(execution_id, None)
            if session:
                session.state = ConnectionState.READY
    
    async def _execute_streaming(
        self,
        execution_id: str,
        model_id: str,
        prompt: str,
        backend,
        temperature: float,
        max_tokens: int,
        context: list
    ):
        """Execute streaming and send tokens to client."""
        start_time = time.time()
        total_tokens = 0
        full_content = []
        
        # Prewarm cache
        self.model_cache.put(model_id, {"model_id": model_id, "prewarmed": True})
        
        try:
            async for response in backend.generate(
                prompt=prompt,
                model=model_id,
                stream=True,
                temperature=temperature,
                max_tokens=max_tokens
            ):
                # Send token chunk
                await self.pool.send_to_session(self.session_id, {
                    "type": "execution.token",
                    "execution_id": execution_id,
                    "token": response.content,
                    "tokens_generated": response.completion_tokens,
                    "is_final": response.done,
                    "timestamp": time.time()
                })
                
                full_content.append(response.content)
                total_tokens = response.completion_tokens
                
                if response.done:
                    latency_ms = (time.time() - start_time) * 1000
                    await self.pool.send_to_session(self.session_id, {
                        "type": "execution.completed",
                        "execution_id": execution_id,
                        "content": "".join(full_content),
                        "tokens_generated": total_tokens,
                        "latency_ms": latency_ms,
                        "model": model_id,
                        "timestamp": time.time()
                    })
        
        except asyncio.CancelledError:
            await self.pool.send_to_session(self.session_id, {
                "type": "execution.cancelled",
                "execution_id": execution_id,
                "tokens_generated": total_tokens
            })
        except Exception as e:
            await self.pool.send_to_session(self.session_id, {
                "type": "execution.error",
                "execution_id": execution_id,
                "error": str(e)
            })
    
    async def _handle_cancel(self, websocket: WebSocket, message: dict):
        """Cancel an ongoing execution."""
        execution_id = message.get("execution_id")
        
        if execution_id and execution_id in self._running_tasks:
            self._running_tasks[execution_id].cancel()
            await self.pool.send_to_session(self.session_id, {
                "type": "execution.cancelled",
                "execution_id": execution_id
            })
    
    async def _handle_subscribe(self, websocket: WebSocket, message: dict):
        """Subscribe to model updates."""
        model_id = message.get("model_id")
        if model_id:
            self.pool.subscribe_to_model(self.session_id, model_id)
            await self.pool.send_to_session(self.session_id, {
                "type": "subscribed",
                "model_id": model_id
            })
    
    async def _handle_unsubscribe(self, websocket: WebSocket, message: dict):
        """Unsubscribe from model updates."""
        model_id = message.get("model_id")
        if model_id:
            self.pool.unsubscribe_from_model(self.session_id, model_id)
            await self.pool.send_to_session(self.session_id, {
                "type": "unsubscribed",
                "model_id": model_id
            })
    
    async def _handle_ping(self, websocket: WebSocket, message: dict):
        """Handle ping for connection health."""
        await self.pool.send_to_session(self.session_id, {
            "type": "pong",
            "timestamp": time.time()
        })
    
    async def _handle_prewarm(self, websocket: WebSocket, message: dict):
        """Prewarm models in cache."""
        model_ids = message.get("models", [])
        self.model_cache.prewarm(model_ids)
        await self.pool.send_to_session(self.session_id, {
            "type": "prewarm.completed",
            "models": model_ids,
            "cached": list(self.model_cache._cache.keys())
        })
    
    async def _handle_switch_model(self, websocket: WebSocket, message: dict):
        """Switch to a different model (instant via cache)."""
        new_model = message.get("model_id")
        
        if not self.model_cache.is_cached(new_model):
            # Warm up the new model
            await self.pool.send_to_session(self.session_id, {
                "type": "execution.status",
                "status": "warming_model",
                "message": f"Warming {new_model}..."
            })
            self.model_cache.put(new_model, {"model_id": new_model})
            await asyncio.sleep(0.1)  # Simulate load
        
        session = self.pool.get_session(self.session_id)
        if session:
            session.current_model = new_model
        
        await self.pool.send_to_session(self.session_id, {
            "type": "model.switched",
            "model_id": new_model,
            "instant": self.model_cache.is_cached(new_model)
        })


# Global pool instance
_ws_pool: Optional[WebSocketPool] = None


def get_websocket_pool() -> WebSocketPool:
    """Get the global WebSocket pool instance."""
    global _ws_pool
    if _ws_pool is None:
        _ws_pool = WebSocketPool()
    return _ws_pool


async def websocket_endpoint(websocket: WebSocket, session_id: str = None):
    """
    Main WebSocket endpoint handler.
    Provides ChatGPT-level bidirectional streaming.
    """
    from .streaming import get_executor
    
    pool = get_websocket_pool()
    backend_manager = get_backend_manager()
    executor = get_executor()
    
    session_id = session_id or str(uuid.uuid4())
    
    # Connect to pool
    await pool.connect(websocket, session_id)
    
    # Create session handler
    session = ChatGPTLevelSession(
        pool=pool,
        backend_manager=backend_manager,
        model_cache=executor._cache,
        session_id=session_id
    )
    
    try:
        while True:
            # Receive message
            data = await websocket.receive_text()
            
            try:
                message = json.loads(data)
                await session.handle_message(websocket, message)
            except json.JSONDecodeError:
                await pool.send_to_session(session_id, {
                    "type": "error",
                    "error": "Invalid JSON"
                })
    
    except WebSocketDisconnect:
        pass
    except Exception as e:
        await pool.send_to_session(session_id, {
            "type": "error",
            "error": str(e)
        })
    finally:
        await pool.disconnect(session_id)
