"""
JWT Authentication - Secure user authentication and authorization.
Handles token-based auth, password hashing, and session management.
"""
import asyncio
import hashlib
import secrets
import time
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from enum import Enum
from functools import wraps

try:
    import jwt
    JWT_AVAILABLE = True
except ImportError:
    JWT_AVAILABLE = False

try:
    import bcrypt
    BCRYPT_AVAILABLE = True
except ImportError:
    BCRYPT_AVAILABLE = False


class AuthError(Exception):
    """Authentication error."""
    pass


class Tier(Enum):
    """User tiers for access control."""
    STANDARD = "standard"
    PREMIUM = "premium"
    RESEARCH = "research"
    ADMIN = "admin"


@dataclass
class User:
    """Authenticated user context."""
    id: str
    username: str
    email: Optional[str]
    tier: str
    permissions: List[str]
    created_at: float


@dataclass
class TokenData:
    """JWT token payload."""
    sub: str  # user_id
    username: str
    tier: str
    exp: float  # expiration timestamp
    iat: float  # issued at


class PasswordHasher:
    """Secure password hashing using bcrypt."""
    
    @staticmethod
    def hash(password: str) -> str:
        """Hash a password."""
        if not BCRYPT_AVAILABLE:
            # Fallback to SHA256 (not recommended for production)
            return hashlib.sha256(password.encode()).hexdigest()
        
        salt = bcrypt.gensalt(rounds=12)
        return bcrypt.hashpw(password.encode(), salt).decode()
    
    @staticmethod
    def verify(password: str, hashed: str) -> bool:
        """Verify a password against its hash."""
        if not BCRYPT_AVAILABLE:
            return hashlib.sha256(password.encode()).hexdigest() == hashed
        
        return bcrypt.checkpw(password.encode(), hashed.encode())


class JWTManager:
    """
    JWT token manager.
    
    Features:
    - Access token generation (short-lived, 15 min)
    - Refresh token generation (long-lived, 7 days)
    - Token verification
    - Automatic refresh
    """
    
    def __init__(
        self,
        secret_key: str = None,
        algorithm: str = "HS256",
        access_token_expire_minutes: int = 15,
        refresh_token_expire_days: int = 7
    ):
        if not JWT_AVAILABLE:
            raise RuntimeError("PyJWT not installed. Run: pip install pyjwt")
        
        # Generate secure secret if not provided
        self.secret_key = secret_key or secrets.token_urlsafe(32)
        self.algorithm = algorithm
        self.access_expire = access_token_expire_minutes * 60
        self.refresh_expire = refresh_token_expire_days * 86400
    
    def create_access_token(
        self,
        user_id: str,
        username: str,
        tier: str,
        additional_claims: Dict[str, Any] = None
    ) -> str:
        """Create a short-lived access token."""
        now = time.time()
        
        payload = {
            "sub": user_id,
            "username": username,
            "tier": tier,
            "type": "access",
            "iat": now,
            "exp": now + self.access_expire,
            **(additional_claims or {})
        }
        
        return jwt.encode(payload, self.secret_key, algorithm=self.algorithm)
    
    def create_refresh_token(
        self,
        user_id: str,
        username: str
    ) -> str:
        """Create a long-lived refresh token."""
        now = time.time()
        
        payload = {
            "sub": user_id,
            "username": username,
            "type": "refresh",
            "iat": now,
            "exp": now + self.refresh_expire
        }
        
        return jwt.encode(payload, self.secret_key, algorithm=self.algorithm)
    
    def verify_token(self, token: str, token_type: str = "access") -> TokenData:
        """
        Verify and decode a token.
        Raises AuthError if invalid or expired.
        """
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            
            # Check token type
            if payload.get("type") != token_type:
                raise AuthError(f"Invalid token type: expected {token_type}")
            
            return TokenData(
                sub=payload["sub"],
                username=payload["username"],
                tier=payload.get("tier", "standard"),
                exp=payload["exp"],
                iat=payload["iat"]
            )
        
        except jwt.ExpiredSignatureError:
            raise AuthError("Token has expired")
        except jwt.InvalidTokenError as e:
            raise AuthError(f"Invalid token: {str(e)}")
    
    def refresh_access_token(self, refresh_token: str) -> str:
        """
        Create a new access token from a refresh token.
        """
        token_data = self.verify_token(refresh_token, token_type="refresh")
        
        return self.create_access_token(
            user_id=token_data.sub,
            username=token_data.username,
            tier=token_data.tier
        )


class AuthManager:
    """
    Complete authentication and authorization manager.
    
    Integrates with:
    - JWT for tokens
    - Password hashing
    - User store (in-memory or database)
    - Session management
    """
    
    def __init__(
        self,
        jwt_manager: JWTManager = None,
        secret_key: str = None
    ):
        self.jwt = jwt_manager or JWTManager(secret_key=secret_key)
        self.password = PasswordHasher()
        
        # In-memory user store (replace with DB in production)
        self._users: Dict[str, Dict[str, Any]] = {}
        self._sessions: Dict[str, Dict[str, Any]] = {}
        
        # Initialize demo users
        self._init_demo_users()
    
    def _init_demo_users(self):
        """Initialize demo users."""
        self.create_user(
            username="admin",
            password="admin123",
            email="admin@ritual.ai",
            tier="admin",
            permissions=["*"]
        )
        self.create_user(
            username="demo",
            password="demo123",
            email="demo@ritual.ai",
            tier="standard",
            permissions=["execute", "stream", "view"]
        )
        self.create_user(
            username="premium",
            password="premium123",
            email="premium@ritual.ai",
            tier="premium",
            permissions=["execute", "stream", "view", "prewarm"]
        )
    
    def create_user(
        self,
        username: str,
        password: str,
        email: str = None,
        tier: str = "standard",
        permissions: List[str] = None
    ) -> Dict[str, Any]:
        """Create a new user."""
        # Check if user exists
        if any(u.get("username") == username for u in self._users.values()):
            raise AuthError(f"User '{username}' already exists")
        
        user_id = secrets.token_urlsafe(16)
        
        user = {
            "id": user_id,
            "username": username,
            "email": email,
            "hashed_password": self.password.hash(password),
            "tier": tier,
            "permissions": permissions or ["execute", "view"],
            "created_at": time.time(),
            "last_login": None
        }
        
        self._users[user_id] = user
        return self._public_user(user)
    
    def get_user(self, user_id: str = None, username: str = None) -> Optional[Dict[str, Any]]:
        """Get user by ID or username."""
        if user_id:
            return self._users.get(user_id)
        if username:
            for user in self._users.values():
                if user.get("username") == username:
                    return user
        return None
    
    def authenticate(self, username: str, password: str) -> Dict[str, Any]:
        """
        Authenticate user and return tokens.
        """
        user = self.get_user(username=username)
        
        if not user:
            raise AuthError("Invalid username or password")
        
        if not self.password.verify(password, user["hashed_password"]):
            raise AuthError("Invalid username or password")
        
        # Update last login
        user["last_login"] = time.time()
        
        # Create tokens
        access_token = self.jwt.create_access_token(
            user_id=user["id"],
            username=user["username"],
            tier=user["tier"]
        )
        refresh_token = self.jwt.create_refresh_token(
            user_id=user["id"],
            username=user["username"]
        )
        
        return {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_type": "bearer",
            "expires_in": self.jwt.access_expire,
            "user": self._public_user(user)
        }
    
    def refresh_token(self, refresh_token: str) -> Dict[str, Any]:
        """
        Refresh access token using refresh token.
        """
        new_access = self.jwt.refresh_access_token(refresh_token)
        
        return {
            "access_token": new_access,
            "token_type": "bearer",
            "expires_in": self.jwt.access_expire
        }
    
    def verify_access_token(self, token: str) -> User:
        """
        Verify access token and return user context.
        """
        token_data = self.jwt.verify_token(token, token_type="access")
        user = self.get_user(user_id=token_data.sub)
        
        if not user:
            raise AuthError("User not found")
        
        return User(
            id=user["id"],
            username=user["username"],
            email=user.get("email"),
            tier=user["tier"],
            permissions=user["permissions"],
            created_at=user["created_at"]
        )
    
    def create_session(
        self,
        user_id: str,
        ip_address: str = None,
        user_agent: str = None
    ) -> str:
        """Create a new session."""
        session_id = secrets.token_urlsafe(32)
        
        self._sessions[session_id] = {
            "user_id": user_id,
            "created_at": time.time(),
            "last_activity": time.time(),
            "ip_address": ip_address,
            "user_agent": user_agent
        }
        
        return session_id
    
    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get session by ID."""
        session = self._sessions.get(session_id)
        if session:
            # Update last activity
            session["last_activity"] = time.time()
        return session
    
    def delete_session(self, session_id: str):
        """Delete a session."""
        self._sessions.pop(session_id, None)
    
    def _public_user(self, user: Dict[str, Any]) -> Dict[str, Any]:
        """Return public user data (no password)."""
        return {
            "id": user["id"],
            "username": user["username"],
            "email": user.get("email"),
            "tier": user["tier"],
            "permissions": user["permissions"],
            "created_at": user["created_at"],
            "last_login": user.get("last_login")
        }


# ==================== Auth Dependencies (FastAPI) ====================

# Global auth manager
_auth_manager: Optional[AuthManager] = None


def get_auth_manager() -> AuthManager:
    """Get global auth manager instance."""
    global _auth_manager
    if _auth_manager is None:
        _auth_manager = AuthManager()
    return _auth_manager


def set_auth_manager(manager: AuthManager):
    """Set global auth manager (for DI)."""
    global _auth_manager
    _auth_manager = manager


# ==================== FastAPI Dependencies ====================

class AuthDependency:
    """FastAPI dependency for authentication."""
    
    @staticmethod
    async def get_current_user(authorization: str = None) -> User:
        """
        Get current authenticated user from Authorization header.
        
        Usage in FastAPI:
            @router.post("/protected")
            async def protected_route(user: User = Depends(AuthDependency.get_current_user)):
                return {"user_id": user.id}
        """
        auth_manager = get_auth_manager()
        
        if not authorization:
            raise AuthError("Missing Authorization header")
        
        # Extract token from "Bearer <token>"
        parts = authorization.split()
        if len(parts) != 2 or parts[0].lower() != "bearer":
            raise AuthError("Invalid Authorization header format")
        
        token = parts[1]
        return auth_manager.verify_access_token(token)
    
    @staticmethod
    async def get_optional_user(authorization: str = None) -> Optional[User]:
        """Get current user if authenticated, None otherwise."""
        try:
            return await AuthDependency.get_current_user(authorization)
        except AuthError:
            return None
    
    @staticmethod
    def require_permission(permission: str):
        """Create dependency that requires specific permission."""
        async def check_permission(user: User = Depends(AuthDependency.get_current_user)) -> User:
            if permission not in user.permissions and "*" not in user.permissions:
                raise AuthError(f"Permission denied: {permission}")
            return user
        return check_permission
    
    @staticmethod
    def require_tier(min_tier: str):
        """Create dependency that requires minimum tier."""
        tier_levels = {"standard": 0, "premium": 1, "research": 2, "admin": 3}
        
        async def check_tier(user: User = Depends(AuthDependency.get_current_user)) -> User:
            user_level = tier_levels.get(user.tier, 0)
            required_level = tier_levels.get(min_tier, 0)
            
            if user_level < required_level:
                raise AuthError(f"Requires {min_tier} tier or higher")
            return user
        return check_tier


# Import Depends for FastAPI
try:
    from fastapi import Depends, Header, HTTPException
    DEPENDS_AVAILABLE = True
except ImportError:
    DEPENDS_AVAILABLE = False


def create_auth_depends():
    """Create FastAPI authentication dependencies."""
    if not DEPENDS_AVAILABLE:
        return None
    
    async def get_user(
        authorization: str = Header(None)
    ) -> User:
        """Dependency to get current authenticated user."""
        if not authorization:
            raise HTTPException(401, "Missing Authorization header")
        
        parts = authorization.split()
        if len(parts) != 2 or parts[0].lower() != "bearer":
            raise HTTPException(401, "Invalid Authorization header format")
        
        try:
            return get_auth_manager().verify_access_token(parts[1])
        except AuthError as e:
            raise HTTPException(401, str(e))
    
    return {"get_user": get_user}
