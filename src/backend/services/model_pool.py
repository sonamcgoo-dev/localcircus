"""
Model Pool Manager - Prewarm pool for instant model switching.
Maintains ready-to-run model instances with zero reload latency.
"""
import asyncio
import time
import hashlib
from typing import Dict, Optional, Set, List, Any
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict
import threading


class ModelState(Enum):
    """Model pool state."""
    UNLOADED = "unloaded"
    LOADING = "loading"
    READY = "ready"
    BUSY = "busy"
    COOLING = "cooling"
    ERROR = "error"


@dataclass
class ModelInstance:
    """A loaded model instance in the pool."""
    model_id: str
    backend_name: str
    state: ModelState = ModelState.UNLOADED
    loaded_at: float = 0.0
    last_used: float = 0.0
    use_count: int = 0
    load_time_ms: float = 0.0
    memory_mb: float = 0.0
    instance: Any = None  # Backend-specific instance handle
    error: Optional[str] = None
    
    def to_dict(self) -> dict:
        return {
            "model_id": self.model_id,
            "backend_name": self.backend_name,
            "state": self.state.value,
            "loaded_at": self.loaded_at,
            "last_used": self.last_used,
            "use_count": self.use_count,
            "load_time_ms": self.load_time_ms,
            "memory_mb": self.memory_mb,
            "ready": self.state == ModelState.READY
        }


@dataclass
class PoolConfig:
    """Configuration for the model pool."""
    max_concurrent_loads: int = 2
    default_pool_size: int = 3
    idle_timeout_seconds: float = 300.0  # 5 minutes
    prewarm_on_start: bool = True
    max_memory_mb: float = 40_000  # 40GB default
    cooling_grace_period: float = 60.0  # seconds before unload after idle


class ModelPool:
    """
    Prewarm pool for instant model switching.
    
    Features:
    - Preload models on startup
    - LRU eviction when memory constrained
    - Automatic cooling/unloading of idle models
    - Instant switching between cached models
    - Background prewarm scheduling
    """
    
    def __init__(self, config: PoolConfig = None):
        self.config = config or PoolConfig()
        
        # Model instances: model_id -> ModelInstance
        self._instances: Dict[str, ModelInstance] = {}
        
        # Backend pools: backend_name -> {model_id -> instance}
        self._backend_pools: Dict[str, Dict[str, ModelInstance]] = defaultdict(dict)
        
        # Requests waiting for model load
        self._load_queue: asyncio.Queue = asyncio.Queue()
        
        # Active loads
        self._active_loads: Set[str] = set()
        
        # Lock for thread safety
        self._lock = asyncio.Lock()
        
        # Background tasks
        self._cleanup_task: Optional[asyncio.Task] = None
        self._prewarm_task: Optional[asyncio.Task] = None
        
        # Statistics
        self._stats = {
            "total_requests": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "loads_started": 0,
            "loads_completed": 0,
            "prewarm_operations": 0
        }
    
    async def start(self):
        """Start background maintenance tasks."""
        if self._cleanup_task is None:
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        if self._prewarm_task is None and self.config.prewarm_on_start:
            self._prewarm_task = asyncio.create_task(self._prewarm_loop())
    
    async def stop(self):
        """Stop background tasks and unload all models."""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        if self._prewarm_task:
            self._prewarm_task.cancel()
            try:
                await self._prewarm_task
            except asyncio.CancelledError:
                pass
        
        # Unload all
        async with self._lock:
            self._instances.clear()
            self._backend_pools.clear()
    
    # ==================== Core API ====================
    
    async def get_model(self, model_id: str, backend_name: str = "mock") -> ModelInstance:
        """
        Get a model instance, loading it if necessary.
        Returns instantly if model is already cached.
        """
        self._stats["total_requests"] += 1
        
        async with self._lock:
            # Check if already loaded
            if model_id in self._instances:
                instance = self._instances[model_id]
                if instance.state == ModelState.READY:
                    self._stats["cache_hits"] += 1
                    instance.last_used = time.time()
                    instance.use_count += 1
                    return instance
                elif instance.state == ModelState.LOADING:
                    # Wait for loading to complete
                    pass
                elif instance.state == ModelState.BUSY:
                    # Model is busy, might need to wait
                    pass
            else:
                self._stats["cache_misses"] += 1
        
        # Need to load - queue it
        await self._ensure_model_loaded(model_id, backend_name)
        
        # Return the instance (it's being loaded)
        async with self._lock:
            return self._instances.get(model_id)
    
    async def _ensure_model_loaded(self, model_id: str, backend_name: str):
        """Ensure a model is loaded, evicting if necessary."""
        async with self._lock:
            if model_id in self._instances:
                return
            
            # Check if loading
            if model_id in self._active_loads:
                return
            
            # Evict if at memory limit
            await self._maybe_evict()
            
            # Create instance
            instance = ModelInstance(
                model_id=model_id,
                backend_name=backend_name,
                state=ModelState.LOADING
            )
            self._instances[model_id] = instance
            self._backend_pools[backend_name][model_id] = instance
            self._active_loads.add(model_id)
        
        # Load in background
        self._stats["loads_started"] += 1
        asyncio.create_task(self._load_model(model_id, backend_name))
    
    async def _load_model(self, model_id: str, backend_name: str):
        """Load a model into memory."""
        load_start = time.time()
        instance = None
        
        async with self._lock:
            instance = self._instances.get(model_id)
            if not instance:
                return
        
        try:
            # Simulate model loading (in real impl, this would call backend)
            # For Ollama: await ollama.load(model_id)
            # For vLLM: the model is already loaded at startup
            await asyncio.sleep(0.1)  # Simulate load time
            
            load_time = (time.time() - load_start) * 1000
            
            async with self._lock:
                if model_id in self._instances:
                    inst = self._instances[model_id]
                    inst.state = ModelState.READY
                    inst.loaded_at = time.time()
                    inst.last_used = time.time()
                    inst.load_time_ms = load_time
                    inst.memory_mb = self._estimate_model_memory(model_id)
                    self._stats["loads_completed"] += 1
                self._active_loads.discard(model_id)
        
        except Exception as e:
            async with self._lock:
                if model_id in self._instances:
                    self._instances[model_id].state = ModelState.ERROR
                    self._instances[model_id].error = str(e)
                self._active_loads.discard(model_id)
    
    async def release_model(self, model_id: str):
        """Release a model back to the pool (mark as ready)."""
        async with self._lock:
            if model_id in self._instances:
                self._instances[model_id].state = ModelState.READY
    
    async def prewarm(self, model_ids: List[str], backend_name: str = "mock"):
        """
        Prewarm models in the pool.
        Loads models in background without blocking.
        """
        self._stats["prewarm_operations"] += 1
        
        for model_id in model_ids:
            if model_id not in self._instances:
                await self._ensure_model_loaded(model_id, backend_name)
    
    async def prewarm_priority(self, model_ids: List[str], backend_name: str = "mock"):
        """
        Prewarm with priority - load immediately.
        Used for instant switching scenarios.
        """
        for model_id in model_ids:
            if model_id not in self._instances:
                async with self._lock:
                    self._active_loads.add(model_id)
                
                # Load immediately
                instance = ModelInstance(
                    model_id=model_id,
                    backend_name=backend_name,
                    state=ModelState.LOADING
                )
                async with self._lock:
                    self._instances[model_id] = instance
                
                await self._load_model(model_id, backend_name)
    
    def is_cached(self, model_id: str) -> bool:
        """Check if model is ready in cache."""
        return (
            model_id in self._instances and 
            self._instances[model_id].state == ModelState.READY
        )
    
    def get_cached_models(self) -> List[str]:
        """Get list of all cached (ready) models."""
        return [
            mid for mid, inst in self._instances.items()
            if inst.state == ModelState.READY
        ]
    
    # ==================== Memory Management ====================
    
    def _estimate_model_memory(self, model_id: str) -> float:
        """Estimate memory usage based on model size."""
        # Common model sizes (rough estimates in MB)
        size_map = {
            "3.8b": 8000,    # Phi-3 mini
            "7b": 14000,     # Mistral 7B
            "8b": 16000,     # LLaMA 3.1 8B
            "13b": 26000,    # Code LLaMA 13B
            "15b": 30000,    # WizardCoder 15B
            "33b": 66000,    # DeepSeek Coder 33B
            "70b": 140000,   # LLaMA 3.1 70B
            "72b": 144000,   # Qwen2.5 72B
        }
        
        for size, mem in size_map.items():
            if size in model_id:
                return mem
        
        return 10000  # Default 10GB
    
    async def _maybe_evict(self):
        """Evict LRU models if at memory limit."""
        current_memory = sum(
            inst.memory_mb for inst in self._instances.values()
            if inst.state == ModelState.READY
        )
        
        if current_memory < self.config.max_memory_mb:
            return
        
        # Find LRU model
        lru_model = None
        lru_time = float('inf')
        
        for model_id, inst in self._instances.items():
            if inst.state == ModelState.READY and inst.last_used < lru_time:
                lru_model = model_id
                lru_time = inst.last_used
        
        if lru_model:
            await self._evict_model(lru_model)
    
    async def _evict_model(self, model_id: str):
        """Evict a model from the pool."""
        if model_id in self._instances:
            inst = self._instances[model_id]
            backend = inst.backend_name
            
            # Unload from backend
            # In real impl: await backend.unload(model_id)
            
            del self._instances[model_id]
            if backend in self._backend_pools:
                self._backend_pools[backend].pop(model_id, None)
    
    # ==================== Background Tasks ====================
    
    async def _cleanup_loop(self):
        """Background task to cool down idle models."""
        while True:
            try:
                await asyncio.sleep(30)  # Check every 30 seconds
                
                now = time.time()
                async with self._lock:
                    for model_id, inst in list(self._instances.items()):
                        if inst.state == ModelState.READY:
                            idle_time = now - inst.last_used
                            if idle_time > self.config.idle_timeout_seconds:
                                # Move to cooling
                                inst.state = ModelState.COOLING
                                # Could unload after grace period
                                
            except asyncio.CancelledError:
                break
            except Exception:
                pass
    
    async def _prewarm_loop(self):
        """Background task to prewarm common models."""
        common_models = [
            "llama-3.1-8b-instruct",
            "mistral-7b-instruct",
            "phi-3-mini-128k"
        ]
        
        # Initial prewarm
        await self.prewarm(common_models)
        
        # Then periodically refresh
        while True:
            try:
                await asyncio.sleep(60)  # Refresh every minute
                # Keep common models warm
                for model_id in common_models:
                    if not self.is_cached(model_id):
                        await self.prewarm([model_id])
            except asyncio.CancelledError:
                break
            except Exception:
                pass
    
    # ==================== Stats & Debug ====================
    
    def get_stats(self) -> dict:
        """Get pool statistics."""
        return {
            **self._stats,
            "cache_hit_rate": (
                self._stats["cache_hits"] / self._stats["total_requests"]
                if self._stats["total_requests"] > 0 else 0
            ),
            "models_loaded": len([
                m for m in self._instances.values()
                if m.state == ModelState.READY
            ]),
            "models_loading": len(self._active_loads),
            "total_memory_mb": sum(
                m.memory_mb for m in self._instances.values()
                if m.state == ModelState.READY
            )
        }
    
    def get_status(self) -> dict:
        """Get detailed pool status."""
        return {
            "config": {
                "max_memory_mb": self.config.max_memory_mb,
                "idle_timeout_seconds": self.config.idle_timeout_seconds,
                "prewarm_on_start": self.config.prewarm_on_start
            },
            "instances": {
                mid: inst.to_dict() 
                for mid, inst in self._instances.items()
            },
            "stats": self.get_stats()
        }


# Global pool instance
_pool: Optional[ModelPool] = None


def get_model_pool() -> ModelPool:
    """Get the global model pool instance."""
    global _pool
    if _pool is None:
        _pool = ModelPool()
    return _pool
