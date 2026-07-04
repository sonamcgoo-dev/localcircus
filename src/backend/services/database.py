"""
PostgreSQL Persistence Layer - Full database models and operations.
Stores users, executions, conversations, and audit logs.
"""
import asyncio
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from dataclasses import dataclass, field
from enum import Enum
import uuid
import json

try:
    from sqlalchemy import create_engine, Column, String, Integer, Float, Boolean, DateTime, Text, ForeignKey, JSON, Index, select, func, desc
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker, relationship
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False


Base = declarative_base() if SQLALCHEMY_AVAILABLE else None


# ==================== SQLAlchemy Models ====================

if SQLALCHEMY_AVAILABLE:
    
    class User(Base):
        __tablename__ = "users"
        
        id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
        username = Column(String(255), unique=True, nullable=False, index=True)
        email = Column(String(255), unique=True, nullable=True)
        hashed_password = Column(String(255), nullable=False)
        tier = Column(String(50), default="standard")  # standard, premium, research, admin
        is_active = Column(Boolean, default=True)
        is_verified = Column(Boolean, default=False)
        created_at = Column(DateTime, default=datetime.utcnow)
        updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
        last_login = Column(DateTime, nullable=True)
        
        # Relationships
        executions = relationship("Execution", back_populates="user", cascade="all, delete-orphan")
        conversations = relationship("Conversation", back_populates="user", cascade="all, delete-orphan")
        audit_logs = relationship("AuditLog", back_populates="user", cascade="all, delete-orphan")
        
        def to_dict(self) -> Dict[str, Any]:
            return {
                "id": self.id,
                "username": self.username,
                "email": self.email,
                "tier": self.tier,
                "is_active": self.is_active,
                "is_verified": self.is_verified,
                "created_at": self.created_at.isoformat() if self.created_at else None,
                "last_login": self.last_login.isoformat() if self.last_login else None,
            }
    
    class Execution(Base):
        __tablename__ = "executions"
        
        id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
        user_id = Column(String(36), ForeignKey("users.id"), nullable=False, index=True)
        model_id = Column(String(255), nullable=False, index=True)
        input_data = Column(Text, nullable=False)
        output_data = Column(Text, nullable=True)
        input_tokens = Column(Integer, default=0)
        output_tokens = Column(Integer, default=0)
        latency_ms = Column(Integer, default=0)
        status = Column(String(50), default="completed")  # completed, failed, cancelled
        error_message = Column(Text, nullable=True)
        metadata = Column(JSON, default=dict)
        created_at = Column(DateTime, default=datetime.utcnow, index=True)
        
        # Relationships
        user = relationship("User", back_populates="executions")
        
        # Indexes for common queries
        __table_args__ = (
            Index("ix_executions_user_created", "user_id", "created_at"),
            Index("ix_executions_model_created", "model_id", "created_at"),
        )
        
        def to_dict(self) -> Dict[str, Any]:
            return {
                "id": self.id,
                "user_id": self.user_id,
                "model_id": self.model_id,
                "input_data": self.input_data[:200] + "..." if len(self.input_data) > 200 else self.input_data,
                "output_tokens": self.output_tokens,
                "latency_ms": self.latency_ms,
                "status": self.status,
                "created_at": self.created_at.isoformat() if self.created_at else None,
            }
    
    class Conversation(Base):
        __tablename__ = "conversations"
        
        id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
        user_id = Column(String(36), ForeignKey("users.id"), nullable=False, index=True)
        title = Column(String(255), default="New Chat")
        messages = Column(JSON, default=list)  # [{"role": "user", "content": "...", "timestamp": ...}]
        metadata = Column(JSON, default=dict)
        is_archived = Column(Boolean, default=False)
        created_at = Column(DateTime, default=datetime.utcnow)
        updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
        
        # Relationships
        user = relationship("User", back_populates="conversations")
        
        def to_dict(self) -> Dict[str, Any]:
            return {
                "id": self.id,
                "user_id": self.user_id,
                "title": self.title,
                "message_count": len(self.messages) if self.messages else 0,
                "is_archived": self.is_archived,
                "created_at": self.created_at.isoformat() if self.created_at else None,
                "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            }
    
    class AuditLog(Base):
        __tablename__ = "audit_logs"
        
        id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
        user_id = Column(String(36), ForeignKey("users.id"), nullable=True, index=True)
        action = Column(String(100), nullable=False, index=True)  # login, logout, execute, etc.
        resource = Column(String(255), nullable=True)
        resource_id = Column(String(36), nullable=True)
        details = Column(JSON, default=dict)
        ip_address = Column(String(45), nullable=True)
        user_agent = Column(Text, nullable=True)
        created_at = Column(DateTime, default=datetime.utcnow, index=True)
        
        # Relationships
        user = relationship("User", back_populates="audit_logs")
        
        __table_args__ = (
            Index("ix_audit_user_action", "user_id", "action"),
        )


# ==================== Database Manager ====================

class DatabaseManager:
    """
    PostgreSQL database manager with async support.
    Handles all CRUD operations for the persistence layer.
    """
    
    def __init__(self, database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/ritual"):
        if not SQLALCHEMY_AVAILABLE:
            raise RuntimeError("SQLAlchemy not installed. Run: pip install sqlalchemy asyncpg")
        
        self.database_url = database_url
        self.async_engine = None
        self.async_session_factory = None
        self._initialized = False
    
    async def initialize(self):
        """Initialize database connection and create tables."""
        if self._initialized:
            return
        
        self.async_engine = create_async_engine(
            self.database_url,
            echo=False,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
        )
        
        self.async_session_factory = sessionmaker(
            self.async_engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        
        # Create tables
        async with self.async_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        
        self._initialized = True
    
    async def close(self):
        """Close database connection."""
        if self.async_engine:
            await self.async_engine.dispose()
    
    async def get_session(self) -> AsyncSession:
        """Get a new database session."""
        if not self._initialized:
            await self.initialize()
        return self.async_session_factory()
    
    # ==================== User Operations ====================
    
    async def create_user(
        self,
        username: str,
        hashed_password: str,
        email: str = None,
        tier: str = "standard"
    ) -> User:
        """Create a new user."""
        async with await self.get_session() as session:
            user = User(
                username=username,
                hashed_password=hashed_password,
                email=email,
                tier=tier
            )
            session.add(user)
            await session.commit()
            await session.refresh(user)
            return user
    
    async def get_user_by_id(self, user_id: str) -> Optional[User]:
        """Get user by ID."""
        async with await self.get_session() as session:
            result = await session.execute(select(User).where(User.id == user_id))
            return result.scalar_one_or_none()
    
    async def get_user_by_username(self, username: str) -> Optional[User]:
        """Get user by username."""
        async with await self.get_session() as session:
            result = await session.execute(select(User).where(User.username == username))
            return result.scalar_one_or_none()
    
    async def update_user(self, user_id: str, **kwargs) -> Optional[User]:
        """Update user fields."""
        async with await self.get_session() as session:
            result = await session.execute(select(User).where(User.id == user_id))
            user = result.scalar_one_or_none()
            if user:
                for key, value in kwargs.items():
                    if hasattr(user, key):
                        setattr(user, key, value)
                await session.commit()
                await session.refresh(user)
            return user
    
    async def list_users(self, limit: int = 100, offset: int = 0) -> List[User]:
        """List all users."""
        async with await self.get_session() as session:
            result = await session.execute(
                select(User).order_by(desc(User.created_at)).limit(limit).offset(offset)
            )
            return list(result.scalars().all())
    
    # ==================== Execution Operations ====================
    
    async def create_execution(
        self,
        user_id: str,
        model_id: str,
        input_data: str,
        output_data: str = None,
        input_tokens: int = 0,
        output_tokens: int = 0,
        latency_ms: int = 0,
        status: str = "completed",
        error_message: str = None,
        metadata: Dict = None
    ) -> Execution:
        """Create execution record."""
        async with await self.get_session() as session:
            execution = Execution(
                user_id=user_id,
                model_id=model_id,
                input_data=input_data,
                output_data=output_data,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                latency_ms=latency_ms,
                status=status,
                error_message=error_message,
                metadata=metadata or {}
            )
            session.add(execution)
            await session.commit()
            await session.refresh(execution)
            return execution
    
    async def get_execution(self, execution_id: str) -> Optional[Execution]:
        """Get execution by ID."""
        async with await self.get_session() as session:
            result = await session.execute(select(Execution).where(Execution.id == execution_id))
            return result.scalar_one_or_none()
    
    async def list_user_executions(
        self,
        user_id: str,
        limit: int = 50,
        offset: int = 0,
        model_id: str = None
    ) -> List[Execution]:
        """List user's executions."""
        async with await self.get_session() as session:
            query = select(Execution).where(Execution.user_id == user_id)
            if model_id:
                query = query.where(Execution.model_id == model_id)
            query = query.order_by(desc(Execution.created_at)).limit(limit).offset(offset)
            result = await session.execute(query)
            return list(result.scalars().all())
    
    async def get_execution_stats(
        self,
        user_id: str = None,
        model_id: str = None,
        days: int = 7
    ) -> Dict[str, Any]:
        """Get execution statistics."""
        async with await self.get_session() as session:
            since = datetime.utcnow() - timedelta(days=days)
            
            query = select(
                func.count(Execution.id).label("total_executions"),
                func.sum(Execution.output_tokens).label("total_tokens"),
                func.avg(Execution.latency_ms).label("avg_latency_ms"),
            ).where(Execution.created_at >= since)
            
            if user_id:
                query = query.where(Execution.user_id == user_id)
            if model_id:
                query = query.where(Execution.model_id == model_id)
            
            result = await session.execute(query)
            row = result.one()
            
            return {
                "total_executions": row.total_executions or 0,
                "total_tokens": row.total_tokens or 0,
                "avg_latency_ms": float(row.avg_latency_ms or 0),
                "days": days
            }
    
    # ==================== Conversation Operations ====================
    
    async def create_conversation(
        self,
        user_id: str,
        title: str = "New Chat"
    ) -> Conversation:
        """Create a new conversation."""
        async with await self.get_session() as session:
            conv = Conversation(
                user_id=user_id,
                title=title,
                messages=[]
            )
            session.add(conv)
            await session.commit()
            await session.refresh(conv)
            return conv
    
    async def get_conversation(self, conv_id: str) -> Optional[Conversation]:
        """Get conversation by ID."""
        async with await self.get_session() as session:
            result = await session.execute(select(Conversation).where(Conversation.id == conv_id))
            return result.scalar_one_or_none()
    
    async def list_user_conversations(
        self,
        user_id: str,
        limit: int = 50,
        offset: int = 0,
        include_archived: bool = False
    ) -> List[Conversation]:
        """List user's conversations."""
        async with await self.get_session() as session:
            query = select(Conversation).where(Conversation.user_id == user_id)
            if not include_archived:
                query = query.where(Conversation.is_archived == False)
            query = query.order_by(desc(Conversation.updated_at)).limit(limit).offset(offset)
            result = await session.execute(query)
            return list(result.scalars().all())
    
    async def add_message_to_conversation(
        self,
        conv_id: str,
        role: str,
        content: str,
        metadata: Dict = None
    ) -> Optional[Conversation]:
        """Add a message to conversation."""
        async with await self.get_session() as session:
            result = await session.execute(select(Conversation).where(Conversation.id == conv_id))
            conv = result.scalar_one_or_none()
            if conv:
                messages = conv.messages or []
                messages.append({
                    "role": role,
                    "content": content,
                    "timestamp": datetime.utcnow().isoformat(),
                    **(metadata or {})
                })
                conv.messages = messages
                conv.updated_at = datetime.utcnow()
                await session.commit()
                await session.refresh(conv)
            return conv
    
    async def update_conversation(self, conv_id: str, **kwargs) -> Optional[Conversation]:
        """Update conversation fields."""
        async with await self.get_session() as session:
            result = await session.execute(select(Conversation).where(Conversation.id == conv_id))
            conv = result.scalar_one_or_none()
            if conv:
                for key, value in kwargs.items():
                    if hasattr(conv, key):
                        setattr(conv, key, value)
                conv.updated_at = datetime.utcnow()
                await session.commit()
                await session.refresh(conv)
            return conv
    
    # ==================== Audit Log Operations ====================
    
    async def create_audit_log(
        self,
        action: str,
        user_id: str = None,
        resource: str = None,
        resource_id: str = None,
        details: Dict = None,
        ip_address: str = None,
        user_agent: str = None
    ) -> AuditLog:
        """Create audit log entry."""
        async with await self.get_session() as session:
            log = AuditLog(
                user_id=user_id,
                action=action,
                resource=resource,
                resource_id=resource_id,
                details=details or {},
                ip_address=ip_address,
                user_agent=user_agent
            )
            session.add(log)
            await session.commit()
            await session.refresh(log)
            return log
    
    async def list_audit_logs(
        self,
        user_id: str = None,
        action: str = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[AuditLog]:
        """List audit logs."""
        async with await self.get_session() as session:
            query = select(AuditLog)
            if user_id:
                query = query.where(AuditLog.user_id == user_id)
            if action:
                query = query.where(AuditLog.action == action)
            query = query.order_by(desc(AuditLog.created_at)).limit(limit).offset(offset)
            result = await session.execute(query)
            return list(result.scalars().all())


# ==================== Database Factory ====================

# Global database instance
_db: Optional[DatabaseManager] = None


def get_database() -> DatabaseManager:
    """Get global database instance."""
    global _db
    if _db is None:
        _db = DatabaseManager()
    return _db


async def init_database(database_url: str = None):
    """Initialize database connection."""
    global _db
    _db = DatabaseManager(database_url)
    await _db.initialize()
    return _db
