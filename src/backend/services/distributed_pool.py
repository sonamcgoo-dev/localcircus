"""
Distributed Model Pool - Redis-backed distributed caching for multi-worker deployment.
Coordinates model prewarming and caching across multiple server instances.
"""
import asyncio
import json
import time
import hashlib
from typing import Dict, List, Optional, Set, Any
from dataclasses import dataclass, field
from enum import Enum
import random

try:
    import redis.asyncio as aioredis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

from .model_pool import ModelPool, ModelState, get_model_pool


class CacheStrategy(Enum):
    """Caching strategies."""
    LRU = "lru"
    LFU = "lfu"
    FIFO = "fifo"
    TTL = "ttl"


@dataclass
class DistributedCacheEntry:
    """Cache entry with metadata."""
    model_id: str
    backend_name: str
    loaded_at: float
    last_used: float
    use_count: int
    size_mb: float
    node_id: str  # Which worker has this model


@dataclass
class PoolStats:
    """Aggregated pool statistics."""
    total_models: int
    models_by_node: Dict[str, int]
    cache_hits: int
    cache_misses: int
    prewarm_requests: int
    nodes_online: int


class DistributedModelPool:
    """
    Redis-backed distributed model pool.
    
    Features:
    - Global model registry across workers
    - Distributed LRU eviction
    - Automatic model discovery
    - Cache invalidation coordination
    - Prewarm broadcasting
    """
    
    # Redis key patterns
    CACHE_SET = "dist:models:cached"
    CACHE_INFO = "dist:model:{model_id}"
    NODE_REGISTER = "dist:nodes"
    NODE_LOCK = "dist:lock:{model_id}"
    CACHE_HITS = "dist:stats:hits"
    CACHE_MISSES = "dist:stats:misses"
    PREWARM_CHANNEL = "dist:prewarm"
    
    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        node_id: str = None,
        max_memory_mb: float = 40_000,
        cache_ttl_seconds: int = 3600
    ):
        self.redis_url = redis_url
        self.node_id = node_id or hashlib.md5(str(time.time()).encode()).hexdigest()[:8]
        self.max_memory_mb = max_memory_mb
        self.cache_ttl = cache_ttl_seconds
        
        # Local pool for actual model loading
        self.local_pool = ModelPool()
        
        # Redis connection
        self._redis: Optional[Any] = None
        
        # PubSub for prewarm broadcasting
        self._pubsub: Optional[Any] = None
        
        # Background tasks
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._prewarm_listener: Optional[asyncio.Task] = None
    
    async def connect(self):
        """Connect to Redis and start background tasks."""
        if not REDIS_AVAILABLE:
            raise RuntimeError("redis.asyncio not installed. Run: pip install redis")
        
        self._redis = aioredis.from_url(
            self.redis_url,
            encoding="utf-8",
            decode_responses=True
        )
        
        # Register this node
        await self._register_node()
        
        # Start heartbeat
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        
        # Start prewarm listener
        self._prewarm_listener = asyncio.create_task(self._prewarm_listener_loop())
    
    async def disconnect(self):
        """Disconnect from Redis and cleanup."""
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        if self._prewarm_listener:
            self._prewarm_listener.cancel()
        
        # Unregister node
        if self._redis:
            await self._redis.srem(self.NODE_REGISTER, self.node_id)
            await self._redis.close()
    
    # ==================== Node Registration ====================
    
    async def _register_node(self):
        """Register this node with Redis."""
        node_info = json.dumps({
            "node_id": self.node_id,
            "registered_at": time.time(),
            "last_heartbeat": time.time(),
            "models_loaded": []
        })
        await self._redis.hset(self.NODE_REGISTER, self.node_id, node_info)
    
    async def _heartbeat_loop(self):
        """Send periodic heartbeats to show node is alive."""
        while True:
            try:
                await asyncio.sleep(10)  # Heartbeat every 10 seconds
                
                node_info = await self._redis.hget(self.NODE_REGISTER, self.node_id)
                if node_info:
                    data = json.loads(node_info)
                    data["last_heartbeat"] = time.time()
                    data["models_loaded"] = self.local_pool.get_cached_models()
                    await self._redis.hset(self.NODE_REGISTER, self.node_id, json.dumps(data))
            except asyncio.CancelledError:
                break
            except Exception:
                pass
    
    async def _prewarm_listener_loop(self):
        """Listen for prewarm broadcasts."""
        self._pubsub = self._redis.pubsub()
        await self._pubsub.subscribe(self.PREWARM_CHANNEL)
        
        try:
            async for message in self._pubsub.listen():
                if message["type"] == "message":
                    model_ids = json.loads(message["data"])
                    # Prewarm models locally
                    await self.local_pool.prewarm(model_ids)
        except asyncio.CancelledError:
            pass
        except Exception:
            pass
        finally:
            await self._pubsub.unsubscribe(self.PREWARM_CHANNEL)
    
    # ==================== Core API ====================
    
    async def get_model(self, model_id: str, backend_name: str = "mock") -> Optional[DistributedCacheEntry]:
        """
        Get a model from the distributed cache.
        Returns entry with node info, or None if not cached.
        """
        # Check if model is cached globally
        cached = await self._redis.sismember(self.CACHE_SET, model_id)
        
        if not cached:
            await self._increment_misses()
            return None
        
        # Get cache info
        cache_key = self.CACHE_INFO.format(model_id=model_id)
        info_json = await self._redis.get(cache_key)
        
        if not info_json:
            return None
        
        info = json.loads(info_json)
        
        # Check if it's on this node
        if info["node_id"] == self.node_id:
            # Check local cache
            if self.local_pool.is_cached(model_id):
                await self._update_usage(model_id)
                await self._increment_hits()
                return DistributedCacheEntry(**info)
        
        await self._increment_hits()
        return DistributedCacheEntry(**info)
    
    async def load_model(self, model_id: str, backend_name: str = "mock") -> bool:
        """
        Load a model and register it in the distributed cache.
        Returns True if loaded successfully.
        """
        # Check if already cached globally
        if await self._redis.sismember(self.CACHE_SET, model_id):
            return True
        
        # Acquire lock to prevent race conditions
        lock_key = self.NODE_LOCK.format(model_id=model_id)
        lock_acquired = await self._redis.set(lock_key, self.node_id, nx=True, ex=30)
        
        if not lock_acquired:
            # Another node is loading
            # Wait and check
            for _ in range(30):  # Wait up to 30 seconds
                await asyncio.sleep(1)
                if await self._redis.sismember(self.CACHE_SET, model_id):
                    return True
            return False
        
        try:
            # Load on local pool
            await self.local_pool.get_model(model_id, backend_name)
            
            # Register in distributed cache
            entry = DistributedCacheEntry(
                model_id=model_id,
                backend_name=backend_name,
                loaded_at=time.time(),
                last_used=time.time(),
                use_count=1,
                size_mb=self.local_pool._estimate_model_memory(model_id),
                node_id=self.node_id
            )
            
            cache_key = self.CACHE_INFO.format(model_id=model_id)
            await self._redis.setex(cache_key, self.cache_ttl, json.dumps({
                "model_id": entry.model_id,
                "backend_name": entry.backend_name,
                "loaded_at": entry.loaded_at,
                "last_used": entry.last_used,
                "use_count": entry.use_count,
                "size_mb": entry.size_mb,
                "node_id": entry.node_id
            }))
            
            await self._redis.sadd(self.CACHE_SET, model_id)
            await self._redis.expire(self.CACHE_SET, self.cache_ttl)
            
            return True
        
        finally:
            await self._redis.delete(lock_key)
    
    async def evict_model(self, model_id: str):
        """Evict a model from the distributed cache."""
        # Remove from global set
        await self._redis.srem(self.CACHE_SET, model_id)
        
        # Remove info
        cache_key = self.CACHE_INFO.format(model_id=model_id)
        await self._redis.delete(cache_key)
    
    async def broadcast_prewarm(self, model_ids: List[str]):
        """Broadcast prewarm request to all nodes."""
        await self._redis.publish(self.PREWARM_CHANNEL, json.dumps(model_ids))
    
    # ==================== Usage Tracking ====================
    
    async def _increment_hits(self):
        """Increment cache hit counter."""
        await self._redis.incr(self.CACHE_HITS)
    
    async def _increment_misses(self):
        """Increment cache miss counter."""
        await self._redis.incr(self.CACHE_MISSES)
    
    async def _update_usage(self, model_id: str):
        """Update usage statistics for a model."""
        cache_key = self.CACHE_INFO.format(model_id=model_id)
        info_json = await self._redis.get(cache_key)
        
        if info_json:
            info = json.loads(info_json)
            info["last_used"] = time.time()
            info["use_count"] = info.get("use_count", 0) + 1
            await self._redis.setex(cache_key, self.cache_ttl, json.dumps(info))
    
    # ==================== Statistics ====================
    
    async def get_stats(self) -> PoolStats:
        """Get aggregated pool statistics."""
        # Get all nodes
        nodes = await self._redis.hgetall(self.NODE_REGISTER)
        
        models_by_node: Dict[str, int] = {}
        for node_id, info_json in nodes.items():
            try:
                info = json.loads(info_json)
                models_by_node[node_id] = len(info.get("models_loaded", []))
            except:
                pass
        
        # Get all cached models
        cached_models = await self._redis.smembers(self.CACHE_SET)
        
        # Get stats
        hits = int(await self._redis.get(self.CACHE_HITS) or 0)
        misses = int(await self._redis.get(self.CACHE_MISSES) or 0)
        
        return PoolStats(
            total_models=len(cached_models),
            models_by_node=models_by_node,
            cache_hits=hits,
            cache_misses=misses,
            prewarm_requests=0,  # TODO: track this
            nodes_online=len(nodes)
        )
    
    async def get_cached_models(self) -> List[str]:
        """Get list of all globally cached models."""
        return list(await self._redis.smembers(self.CACHE_SET))
    
    async def get_node_models(self, node_id: str) -> List[str]:
        """Get models loaded on a specific node."""
        info_json = await self._redis.hget(self.NODE_REGISTER, node_id)
        if info_json:
            info = json.loads(info_json)
            return info.get("models_loaded", [])
        return []


# In-memory fallback
class LocalModelPool:
    """Local-only pool when Redis unavailable."""
    
    def __init__(self):
        self.local_pool = ModelPool()
    
    async def connect(self):
        pass
    
    async def disconnect(self):
        pass
    
    async def get_model(self, model_id: str, backend: str = "mock"):
        return await self.local_pool.get_model(model_id, backend)
    
    async def load_model(self, model_id: str, backend: str = "mock") -> bool:
        await self.local_pool.get_model(model_id, backend)
        return True
    
    async def evict_model(self, model_id: str):
        pass
    
    async def broadcast_prewarm(self, model_ids: List[str]):
        await self.local_pool.prewarm(model_ids)
    
    async def get_stats(self) -> PoolStats:
        stats = self.local_pool.get_stats()
        return PoolStats(
            total_models=stats["models_loaded"],
            models_by_node={"local": stats["models_loaded"]},
            cache_hits=stats["cache_hits"],
            cache_misses=stats["cache_misses"],
            prewarm_requests=stats["prewarm_operations"],
            nodes_online=1
        )
    
    async def get_cached_models(self) -> List[str]:
        return self.local_pool.get_cached_models()


# Factory
def create_distributed_pool(redis_url: str = None) -> Any:
    """Create appropriate pool based on Redis availability."""
    if redis_url and REDIS_AVAILABLE:
        return DistributedModelPool(redis_url=redis_url)
    return LocalModelPool()


# Global pool
_pool: Optional[Any] = None


async def get_distributed_pool(redis_url: str = None) -> Any:
    """Get global distributed pool."""
    global _pool
    if _pool is None:
        _pool = create_distributed_pool(redis_url)
        await _pool.connect()
    return _pool
