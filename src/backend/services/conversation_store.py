"""
Conversation Memory Store - Redis-backed persistent chat history.
Stores conversation history for seamless context across sessions.
"""
import asyncio
import json
import time
from typing import List, Optional, Dict, Any
from dataclasses import dataclass
from enum import Enum
import hashlib

try:
    import redis.asyncio as aioredis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False


class MessageRole(Enum):
    USER = "user"
    ASSISTANT = "assistant"
    SYSTEM = "system"


@dataclass
class Message:
    """Chat message."""
    role: str
    content: str
    timestamp: float = 0.0
    model_id: Optional[str] = None
    tokens: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "role": self.role,
            "content": self.content,
            "timestamp": self.timestamp,
            "model_id": self.model_id,
            "tokens": self.tokens
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Message":
        return cls(
            role=data["role"],
            content=data["content"],
            timestamp=data.get("timestamp", 0.0),
            model_id=data.get("model_id"),
            tokens=data.get("tokens", 0)
        )


@dataclass
class Conversation:
    """Conversation thread."""
    id: str
    user_id: str
    title: str
    messages: List[Message]
    created_at: float
    updated_at: float
    metadata: Dict[str, Any]
    
    @classmethod
    def create(cls, user_id: str, title: str = "New Chat") -> "Conversation":
        now = time.time()
        conv_id = hashlib.sha256(f"{user_id}{now}".encode()).hexdigest()[:16]
        return cls(
            id=conv_id,
            user_id=user_id,
            title=title,
            messages=[],
            created_at=now,
            updated_at=now,
            metadata={}
        )


class ConversationStore:
    """
    Redis-backed conversation memory store.
    
    Features:
    - Automatic conversation threading by user/session
    - Configurable history limits
    - Message search
    - Conversation summaries (optional)
    - TTL-based auto-cleanup
    """
    
    # Key patterns
    CONV_KEY = "conversation:{conv_id}"
    USER_CONVS_KEY = "user_conversations:{user_id}"
    CONV_INDEX_KEY = "conversation_index"
    
    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        max_messages: int = 100,
        max_history_tokens: int = 16000,
        ttl_days: int = 30
    ):
        self.redis_url = redis_url
        self.max_messages = max_messages
        self.max_history_tokens = max_history_tokens
        self.ttl_seconds = ttl_days * 86400
        self._redis: Optional[Any] = None
    
    async def connect(self):
        """Connect to Redis."""
        if not REDIS_AVAILABLE:
            raise RuntimeError("redis.asyncio not installed. Run: pip install redis")
        self._redis = aioredis.from_url(
            self.redis_url,
            encoding="utf-8",
            decode_responses=True
        )
    
    async def disconnect(self):
        """Disconnect from Redis."""
        if self._redis:
            await self._redis.close()
    
    async def _ensure_connected(self):
        """Ensure Redis connection."""
        if not self._redis:
            await self.connect()
    
    # ==================== Conversation CRUD ====================
    
    async def create_conversation(self, user_id: str, title: str = "New Chat") -> Conversation:
        """Create a new conversation."""
        await self._ensure_connected()
        
        conv = Conversation.create(user_id, title)
        
        # Store conversation metadata
        conv_key = self.CONV_KEY.format(conv_id=conv.id)
        conv_data = {
            "id": conv.id,
            "user_id": conv.user_id,
            "title": conv.title,
            "created_at": str(conv.created_at),
            "updated_at": str(conv.updated_at),
            "message_count": "0"
        }
        
        pipe = self._redis.pipeline()
        pipe.hset(conv_key, mapping=conv_data)
        pipe.expire(conv_key, self.ttl_seconds)
        
        # Add to user's conversation list
        user_convs_key = self.USER_CONVS_KEY.format(user_id=user_id)
        pipe.zadd(user_convs_key, {conv.id: conv.created_at})
        pipe.expire(user_convs_key, self.ttl_seconds)
        
        await pipe.execute()
        
        return conv
    
    async def get_conversation(self, conv_id: str) -> Optional[Conversation]:
        """Get conversation by ID."""
        await self._ensure_connected()
        
        conv_key = self.CONV_KEY.format(conv_id=conv_id)
        conv_data = await self._redis.hgetall(conv_key)
        
        if not conv_data:
            return None
        
        # Load messages
        messages_key = f"{conv_key}:messages"
        message_ids = await self._redis.lrange(messages_key, 0, -1)
        
        messages = []
        for msg_id in message_ids:
            msg_key = f"{conv_key}:msg:{msg_id}"
            msg_data = await self._redis.hgetall(msg_key)
            if msg_data:
                messages.append(Message(
                    role=msg_data["role"],
                    content=msg_data["content"],
                    timestamp=float(msg_data.get("timestamp", 0)),
                    model_id=msg_data.get("model_id"),
                    tokens=int(msg_data.get("tokens", 0))
                ))
        
        return Conversation(
            id=conv_data["id"],
            user_id=conv_data["user_id"],
            title=conv_data["title"],
            messages=messages,
            created_at=float(conv_data["created_at"]),
            updated_at=float(conv_data["updated_at"]),
            metadata={}
        )
    
    async def list_user_conversations(
        self,
        user_id: str,
        limit: int = 20,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """List user's conversations (summary only)."""
        await self._ensure_connected()
        
        user_convs_key = self.USER_CONVS_KEY.format(user_id=user_id)
        conv_ids = await self._redis.zrevrange(
            user_convs_key, offset, offset + limit - 1
        )
        
        result = []
        for conv_id in conv_ids:
            conv_key = self.CONV_KEY.format(conv_id=conv_id)
            conv_data = await self._redis.hgetall(conv_key)
            if conv_data:
                result.append({
                    "id": conv_data["id"],
                    "title": conv_data["title"],
                    "message_count": int(conv_data.get("message_count", 0)),
                    "created_at": float(conv_data["created_at"]),
                    "updated_at": float(conv_data["updated_at"])
                })
        
        return result
    
    async def delete_conversation(self, conv_id: str):
        """Delete a conversation and all its messages."""
        await self._ensure_connected()
        
        conv = await self.get_conversation(conv_id)
        if not conv:
            return
        
        conv_key = self.CONV_KEY.format(conv_id=conv_id)
        messages_key = f"{conv_key}:messages"
        
        # Get all message keys
        message_ids = await self._redis.lrange(messages_key, 0, -1)
        
        # Delete all keys
        keys_to_delete = [conv_key, messages_key] + [
            f"{conv_key}:msg:{msg_id}" for msg_id in message_ids
        ]
        
        pipe = self._redis.pipeline()
        for key in keys_to_delete:
            pipe.delete(key)
        
        # Remove from user's conversation list
        user_convs_key = self.USER_CONVS_KEY.format(user_id=conv.user_id)
        pipe.zrem(user_convs_key, conv_id)
        
        await pipe.execute()
    
    # ==================== Messages ====================
    
    async def add_message(
        self,
        conv_id: str,
        role: str,
        content: str,
        model_id: str = None,
        tokens: int = 0
    ) -> Message:
        """Add a message to conversation."""
        await self._ensure_connected()
        
        now = time.time()
        msg_id = hashlib.md5(f"{conv_id}{now}{content[:50]}".encode()).hexdigest()[:12]
        
        msg = Message(
            role=role,
            content=content,
            timestamp=now,
            model_id=model_id,
            tokens=tokens
        )
        
        conv_key = self.CONV_KEY.format(conv_id=conv_id)
        msg_key = f"{conv_key}:msg:{msg_id}"
        messages_key = f"{conv_key}:messages"
        
        pipe = self._redis.pipeline()
        
        # Store message
        pipe.hset(msg_key, mapping={
            "role": msg.role,
            "content": msg.content,
            "timestamp": str(msg.timestamp),
            "model_id": msg.model_id or "",
            "tokens": str(msg.tokens)
        })
        pipe.expire(msg_key, self.ttl_seconds)
        
        # Add to message list
        pipe.rpush(messages_key, msg_id)
        pipe.expire(messages_key, self.ttl_seconds)
        
        # Update conversation metadata
        pipe.hset(conv_key, mapping={
            "updated_at": str(now),
            "message_count": str(await self._redis.llen(messages_key))
        })
        
        await pipe.execute()
        
        return msg
    
    async def get_recent_messages(
        self,
        conv_id: str,
        limit: int = 50
    ) -> List[Message]:
        """Get recent messages from conversation."""
        await self._ensure_connected()
        
        conv_key = self.CONV_KEY.format(conv_id=conv_id)
        messages_key = f"{conv_key}:messages"
        
        # Get last N messages
        message_ids = await self._redis.lrange(messages_key, -limit, -1)
        
        messages = []
        for msg_id in message_ids:
            msg_key = f"{conv_key}:msg:{msg_id}"
            msg_data = await self._redis.hgetall(msg_key)
            if msg_data:
                messages.append(Message(
                    role=msg_data["role"],
                    content=msg_data["content"],
                    timestamp=float(msg_data.get("timestamp", 0)),
                    model_id=msg_data.get("model_id") or None,
                    tokens=int(msg_data.get("tokens", 0))
                ))
        
        return messages
    
    async def get_context_for_model(
        self,
        conv_id: str,
        max_tokens: int = None
    ) -> List[Dict[str, str]]:
        """
        Get conversation context formatted for LLM.
        Returns list of {role, content} dicts.
        """
        max_tokens = max_tokens or self.max_history_tokens
        
        messages = await self.get_recent_messages(conv_id, limit=self.max_messages)
        
        # Trim to token limit (rough estimate: 4 chars = 1 token)
        context = []
        total_tokens = 0
        
        for msg in reversed(messages):
            msg_tokens = msg.tokens or (len(msg.content) // 4)
            if total_tokens + msg_tokens > max_tokens:
                break
            context.insert(0, {"role": msg.role, "content": msg.content})
            total_tokens += msg_tokens
        
        return context
    
    # ==================== Search ====================
    
    async def search_messages(
        self,
        user_id: str,
        query: str,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Search messages across user's conversations."""
        await self._ensure_connected()
        
        # Get user's conversations
        conv_ids = await self._redis.zrevrange(
            self.USER_CONVS_KEY.format(user_id=user_id), 0, -1
        )
        
        results = []
        for conv_id in conv_ids:
            messages = await self.get_recent_messages(conv_id, limit=100)
            for msg in messages:
                if query.lower() in msg.content.lower():
                    results.append({
                        "conversation_id": conv_id,
                        "role": msg.role,
                        "content": msg.content[:200],  # Truncate
                        "timestamp": msg.timestamp,
                        "model_id": msg.model_id
                    })
                    if len(results) >= limit:
                        return results
        
        return results


# In-memory fallback for when Redis is not available
class InMemoryConversationStore:
    """Simple in-memory store when Redis unavailable."""
    
    def __init__(self):
        self._conversations: Dict[str, Conversation] = {}
        self._user_conversations: Dict[str, List[str]] = {}
    
    async def connect(self):
        pass
    
    async def disconnect(self):
        pass
    
    async def create_conversation(self, user_id: str, title: str = "New Chat") -> Conversation:
        conv = Conversation.create(user_id, title)
        self._conversations[conv.id] = conv
        if user_id not in self._user_conversations:
            self._user_conversations[user_id] = []
        self._user_conversations[user_id].append(conv.id)
        return conv
    
    async def get_conversation(self, conv_id: str) -> Optional[Conversation]:
        return self._conversations.get(conv_id)
    
    async def list_user_conversations(self, user_id: str, limit: int = 20, offset: int = 0) -> List[Dict]:
        conv_ids = self._user_conversations.get(user_id, [])[offset:offset+limit]
        return [
            {"id": cid, "title": self._conversations[cid].title, "message_count": len(self._conversations[cid].messages)}
            for cid in conv_ids if cid in self._conversations
        ]
    
    async def add_message(self, conv_id: str, role: str, content: str, model_id: str = None, tokens: int = 0) -> Message:
        msg = Message(role=role, content=content, timestamp=time.time(), model_id=model_id, tokens=tokens)
        if conv_id in self._conversations:
            self._conversations[conv_id].messages.append(msg)
            self._conversations[conv_id].updated_at = time.time()
        return msg
    
    async def get_recent_messages(self, conv_id: str, limit: int = 50) -> List[Message]:
        conv = self._conversations.get(conv_id)
        return conv.messages[-limit:] if conv else []
    
    async def get_context_for_model(self, conv_id: str, max_tokens: int = 16000) -> List[Dict[str, str]]:
        messages = await self.get_recent_messages(conv_id)
        return [{"role": m.role, "content": m.content} for m in messages]
    
    async def delete_conversation(self, conv_id: str):
        if conv_id in self._conversations:
            user_id = self._conversations[conv_id].user_id
            del self._conversations[conv_id]
            if user_id in self._user_conversations:
                self._user_conversations[user_id].remove(conv_id)


# Factory function
def create_conversation_store(redis_url: str = None) -> ConversationStore:
    """Create appropriate conversation store based on Redis availability."""
    if redis_url and REDIS_AVAILABLE:
        return ConversationStore(redis_url=redis_url)
    return InMemoryConversationStore()


# Global store instance
_store: Optional[ConversationStore] = None


async def get_conversation_store(redis_url: str = None) -> ConversationStore:
    """Get global conversation store instance."""
    global _store
    if _store is None:
        _store = create_conversation_store(redis_url)
        await _store.connect()
    return _store
