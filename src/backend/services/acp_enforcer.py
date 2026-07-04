"""
ACP (Access Control Policy) Enforcement Hooks.
Permission checks before /zoo/run execution.
"""
import asyncio
import time
from typing import Dict, Optional, Set, List, Tuple, Any
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict
import hashlib


class Permission(Enum):
    """Available permissions."""
    EXECUTE_MODEL = "execute:model"
    EXECUTE_ANY_MODEL = "execute:any"
    STREAM_OUTPUT = "stream:output"
    VIEW_METRICS = "metrics:view"
    VIEW_REGISTRY = "registry:view"
    ADMIN_MODELS = "models:admin"
    ADMIN_USERS = "users:admin"
    PREWARM = "pool:prewarm"


class ModelCategory(Enum):
    """Model categories for tiered access."""
    STANDARD = "standard"
    PREMIUM = "premium"
    RESEARCH = "research"
    ADMIN_ONLY = "admin"


@dataclass
class User:
    """User with permissions."""
    user_id: str
    username: str
    permissions: Set[Permission] = field(default_factory=set)
    tier: ModelCategory = ModelCategory.STANDARD
    model_access: Set[str] = field(default_factory=set)  # Specific model IDs
    max_daily_executions: int = 1000
    current_executions: int = 0
    daily_limit_reset: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def can_execute_model(self, model_id: str) -> bool:
        """Check if user can execute a specific model."""
        # Admin can do anything
        if Permission.ADMIN_MODELS in self.permissions:
            return True
        
        # Check specific model access
        if model_id in self.model_access:
            return True
        
        # Check tier-based access
        tier_access = {
            ModelCategory.STANDARD: ["chat", "fast", "local"],
            ModelCategory.PREMIUM: ["chat", "fast", "local", "powerful", "70b"],
            ModelCategory.RESEARCH: ["chat", "fast", "local", "powerful", "research", "analysis"],
        }
        
        allowed_prefixes = tier_access.get(self.tier, [])
        for prefix in allowed_prefixes:
            if prefix in model_id.lower():
                return True
        
        # Check global execute permission
        if Permission.EXECUTE_ANY_MODEL in self.permissions:
            return True
        
        return False
    
    def check_rate_limit(self) -> Tuple[bool, str]:
        """Check if user is within rate limits."""
        now = time.time()
        
        # Reset daily counter if needed
        if now > self.daily_limit_reset:
            self.current_executions = 0
            self.daily_limit_reset = now + 86400  # Reset in 24h
        
        if self.current_executions >= self.max_daily_executions:
            return False, f"Daily limit reached ({self.max_daily_executions})"
        
        return True, ""
    
    def record_execution(self):
        """Record an execution for rate limiting."""
        self.current_executions += 1


@dataclass
class ModelPolicy:
    """Policy configuration for a model."""
    model_id: str
    requires_permission: Optional[Permission] = None
    min_tier: ModelCategory = ModelCategory.STANDARD
    rate_limit_per_user: int = 100  # per hour
    rate_limit_global: int = 1000   # global per hour
    enabled: bool = True
    cooldown_seconds: float = 0.0  # Minimum time between same model runs


class ACPEnforcer:
    """
    Access Control Policy Enforcer.
    
    Hooks into execution flow to enforce:
    - Permission checks before /zoo/run
    - Rate limiting
    - Model tier access
    - Audit logging
    """
    
    def __init__(self):
        # User registry
        self._users: Dict[str, User] = {}
        
        # Model policies
        self._model_policies: Dict[str, ModelPolicy] = {}
        
        # Rate limit tracking: model_id -> {user_id -> (count, window_start)}
        self._rate_limits: Dict[str, Dict[str, Tuple[int, float]]] = defaultdict(dict)
        
        # Global rate limits: model_id -> (count, window_start)
        self._global_limits: Dict[str, Tuple[int, float]] = {}
        
        # Execution cooldowns: session_id:model_id -> last_execution_time
        self._cooldowns: Dict[str, float] = {}
        
        # Audit log
        self._audit_log: List[Dict] = []
        
        # Default permissions
        self._default_permissions = {
            Permission.EXECUTE_MODEL,
            Permission.STREAM_OUTPUT,
            Permission.VIEW_METRICS,
            Permission.VIEW_REGISTRY,
        }
        
        # Initialize default policies
        self._init_default_policies()
        
        # Initialize demo users
        self._init_demo_users()
    
    def _init_default_policies(self):
        """Initialize default model policies."""
        default_models = [
            ("llama-3.1-8b-instruct", ModelCategory.STANDARD),
            ("llama-3.1-70b-instruct", ModelCategory.PREMIUM),
            ("mistral-7b-instruct", ModelCategory.STANDARD),
            ("codellama-13b-instruct", ModelCategory.PREMIUM),
            ("phi-3-mini-128k", ModelCategory.STANDARD),
            ("qwen2.5-72b-instruct", ModelCategory.PREMIUM),
            ("deepseek-coder-33b", ModelCategory.PREMIUM),
            ("wizardcoder-15b", ModelCategory.STANDARD),
        ]
        
        for model_id, tier in default_models:
            self._model_policies[model_id] = ModelPolicy(
                model_id=model_id,
                min_tier=tier,
                rate_limit_per_user=100,
                rate_limit_global=500
            )
    
    def _init_demo_users(self):
        """Initialize demo users for testing."""
        # Standard user - has access to standard tier models
        self._users["user_demo"] = User(
            user_id="user_demo",
            username="demo_user",
            permissions={*self._default_permissions, Permission.EXECUTE_ANY_MODEL},
            tier=ModelCategory.STANDARD,
            model_access=set()
        )
        
        # Premium user
        self._users["user_premium"] = User(
            user_id="user_premium",
            username="premium_user",
            permissions={*self._default_permissions, Permission.EXECUTE_ANY_MODEL},
            tier=ModelCategory.PREMIUM,
            model_access=set()
        )
        
        # Admin user
        self._users["user_admin"] = User(
            user_id="user_admin",
            username="admin",
            permissions=set(Permission),  # All permissions
            tier=ModelCategory.ADMIN_ONLY,
            model_access=set(Permission)
        )
    
    def get_user(self, user_id: str) -> Optional[User]:
        """Get user by ID."""
        return self._users.get(user_id)
    
    def create_user(self, user_id: str, username: str, tier: ModelCategory = ModelCategory.STANDARD) -> User:
        """Create a new user."""
        user = User(
            user_id=user_id,
            username=username,
            permissions=self._default_permissions,
            tier=tier
        )
        self._users[user_id] = user
        return user
    
    def grant_permission(self, user_id: str, permission: Permission):
        """Grant permission to user."""
        if user_id in self._users:
            self._users[user_id].permissions.add(permission)
    
    def revoke_permission(self, user_id: str, permission: Permission):
        """Revoke permission from user."""
        if user_id in self._users:
            self._users[user_id].permissions.discard(permission)
    
    def set_model_policy(self, policy: ModelPolicy):
        """Set or update model policy."""
        self._model_policies[policy.model_id] = policy
    
    async def check_execution(self, session_id: str, model_id: str) -> Tuple[bool, str]:
        """
        Check if execution is allowed.
        Returns (allowed, reason) tuple.
        
        Hook this into /zoo/run before execution.
        """
        user_id = session_id  # Use session as user for simplicity
        
        # Get user (or create default)
        user = self._users.get(user_id)
        if not user:
            user = self._users.get("user_demo")
        
        # 1. Check model policy
        policy = self._model_policies.get(model_id)
        if policy and not policy.enabled:
            self._log_audit(session_id, model_id, "denied", "model_disabled")
            return False, f"Model {model_id} is currently disabled"
        
        # 2. Check user model access
        if not user.can_execute_model(model_id):
            self._log_audit(session_id, model_id, "denied", "insufficient_access")
            return False, f"User does not have access to {model_id}"
        
        # 3. Check tier requirements
        if policy and policy.min_tier.value > user.tier.value:
            self._log_audit(session_id, model_id, "denied", "insufficient_tier")
            return False, f"Requires {policy.min_tier.value} tier or higher"
        
        # 4. Check rate limits (per user)
        if policy:
            user_limit_key = f"{user.user_id}:{model_id}"
            if user_limit_key in self._rate_limits[model_id]:
                count, window_start = self._rate_limits[model_id][user_limit_key]
                if time.time() - window_start < 3600 and count >= policy.rate_limit_per_user:
                    self._log_audit(session_id, model_id, "denied", "rate_limit_user")
                    return False, f"Rate limit exceeded for {model_id}"
            
            # 5. Check global rate limits
            if model_id in self._global_limits:
                count, window_start = self._global_limits[model_id]
                if time.time() - window_start < 3600 and count >= policy.rate_limit_global:
                    self._log_audit(session_id, model_id, "denied", "rate_limit_global")
                    return False, f"Global rate limit exceeded for {model_id}"
            
            # 6. Check cooldown
            cooldown_key = f"{session_id}:{model_id}"
            if cooldown_key in self._cooldowns:
                last_run = self._cooldowns[cooldown_key]
                if time.time() - last_run < policy.cooldown_seconds:
                    remaining = policy.cooldown_seconds - (time.time() - last_run)
                    self._log_audit(session_id, model_id, "denied", "cooldown")
                    return False, f"Cooldown active, retry in {remaining:.1f}s"
        
        # 7. Check daily rate limit
        allowed, reason = user.check_rate_limit()
        if not allowed:
            self._log_audit(session_id, model_id, "denied", "daily_limit")
            return False, reason
        
        # All checks passed
        self._log_audit(session_id, model_id, "allowed", "passed")
        return True, ""
    
    def record_execution(self, session_id: str, model_id: str):
        """Record an execution for rate limiting."""
        user_id = session_id
        user = self._users.get(user_id)
        if user:
            user.record_execution()
        
        # Update rate limit counters
        now = time.time()
        
        # Per-user limit
        user_limit_key = f"{user_id}:{model_id}"
        if user_limit_key not in self._rate_limits[model_id]:
            self._rate_limits[model_id][user_limit_key] = (0, now)
        count, window_start = self._rate_limits[model_id][user_limit_key]
        if now - window_start > 3600:
            self._rate_limits[model_id][user_limit_key] = (1, now)
        else:
            self._rate_limits[model_id][user_limit_key] = (count + 1, window_start)
        
        # Global limit
        if model_id not in self._global_limits:
            self._global_limits[model_id] = (0, now)
        count, window_start = self._global_limits[model_id]
        if now - window_start > 3600:
            self._global_limits[model_id] = (1, now)
        else:
            self._global_limits[model_id] = (count + 1, window_start)
        
        # Update cooldown
        cooldown_key = f"{session_id}:{model_id}"
        self._cooldowns[cooldown_key] = now
    
    def _log_audit(self, session_id: str, model_id: str, result: str, reason: str):
        """Log audit entry."""
        self._audit_log.append({
            "timestamp": time.time(),
            "session_id": session_id,
            "model_id": model_id,
            "result": result,
            "reason": reason
        })
        # Keep last 10000 entries
        if len(self._audit_log) > 10000:
            self._audit_log = self._audit_log[-5000:]
    
    def get_audit_log(self, limit: int = 100) -> List[Dict]:
        """Get recent audit log entries."""
        return self._audit_log[-limit:]
    
    def get_stats(self) -> Dict:
        """Get enforcement statistics."""
        total_allowed = sum(1 for e in self._audit_log if e["result"] == "allowed")
        total_denied = sum(1 for e in self._audit_log if e["result"] == "denied")
        
        return {
            "total_checks": len(self._audit_log),
            "allowed": total_allowed,
            "denied": total_denied,
            "denial_rate": total_denied / len(self._audit_log) if self._audit_log else 0,
            "users": len(self._users),
            "models_with_policies": len(self._model_policies),
        }


# Global enforcer instance
_enforcer: Optional[ACPEnforcer] = None


def get_acp_enforcer() -> ACPEnforcer:
    """Get the global ACP enforcer instance."""
    global _enforcer
    if _enforcer is None:
        _enforcer = ACPEnforcer()
    return _enforcer


# Decorator for ACP enforcement
def acp_enforced(permission: Permission = None):
    """
    Decorator to enforce ACP on endpoint functions.
    
    Usage:
        @router.post("/zoo/run")
        @acp_enforced(Permission.EXECUTE_MODEL)
        async def run_model(request):
            ...
    """
    def decorator(func):
        async def wrapper(*args, **kwargs):
            enforcer = get_acp_enforcer()
            
            # Extract session_id and model_id from args/kwargs
            session_id = kwargs.get("session_id") or (args[0] if args else "anonymous")
            model_id = kwargs.get("model_id") or kwargs.get("request", {}).get("model_id", "unknown")
            
            allowed, reason = await enforcer.check_execution(session_id, model_id)
            if not allowed:
                from fastapi import HTTPException
                raise HTTPException(status_code=403, detail=reason)
            
            result = await func(*args, **kwargs)
            
            # Record successful execution
            enforcer.record_execution(session_id, model_id)
            
            return result
        
        return wrapper
    return decorator
