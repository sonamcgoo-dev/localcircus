# RITUAL Marketplace - Production-Ready AI Platform

**Status: ENTERPRISE READY** - Full stack with persistence, auth, and orchestration.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              FRONTEND                                        │
│   ChatGPT-Style UI │ Markdown │ Syntax Highlight │ Token Speed │ Metrics     │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │ REST / WebSocket
┌──────────────────────────────────────┼────────────────────────────────────────┐
│                                      │                                        │
│  ┌───────────────────────────────────┴────────────────────────────────────┐  │
│  │                         FastAPI Gateway                                 │  │
│  │   Auth (JWT) │ Rate Limit │ Metrics │ CORS │ WebSocket Handler        │  │
│  └───────────────────────────────────┬────────────────────────────────────┘  │
│                                      │                                        │
│  ┌──────────────┐  ┌─────────────────┴─────────────────┐  ┌───────────────┐  │
│  │   Registry   │  │       Execution Layer              │  │   ACP/Auth    │  │
│  │  (tag-idx)  │  │  /zoo/run │ /zoo/stream │ /ws    │  │   (perms)     │  │
│  └──────────────┘  └────────────────┬──────────────────┘  └───────────────┘  │
│                                     │                                          │
│  ┌──────────────────────────────────┼──────────────────────────────────────┐ │
│  │                        Service Layer                                   │   │
│  │  ┌────────────┐  ┌─────────────┐  ┌──────────────┐  ┌────────────────┐  │   │
│  │  │ LLMBackend │  │ ModelPool   │  │ Pipeline     │  │ Conversation   │  │   │
│  │  │(Ollama/vLLM│  │(prewarm/LRU)│  │ Orchestrator │  │ Store(Redis)  │  │   │
│  │  └────────────┘  └─────────────┘  └──────────────┘  └────────────────┘  │   │
│  └──────────────────────────────────────────────────────────────────────────┘   │
│                                     │                                          │
│  ┌──────────────────────────────────┼──────────────────────────────────────┐ │
│  │                        Data Layer                                       │   │
│  │  ┌──────────────┐  ┌────────────┴────┐  ┌────────────────────────────┐  │   │
│  │  │   Redis      │  │  PostgreSQL     │  │  Celery (async tasks)     │  │   │
│  │  │  (cache/q)  │  │  (users/exec)   │  │  (background processing)   │  │   │
│  │  └──────────────┘  └─────────────────┘  └────────────────────────────┘  │   │
│  └──────────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Features Implemented

### ✅ Core (v1)
| Feature | Status | Description |
|---------|--------|-------------|
| Marketplace → /registry/search | ✅ | Tag-indexed model search |
| Run button → /zoo/run | ✅ | ACP-enforced execution |
| Streaming execution | ✅ | SSE + WebSocket token streaming |
| Real registry | ✅ | 8 models with metrics |
| Model caching | ✅ | LRU + prewarm pool |
| LLM backends | ✅ | Ollama, vLLM, OpenAI adapters |
| ACP enforcement | ✅ | Permission + rate limiting |

### ✅ Production (v2) - NEW
| Feature | Status | Description |
|---------|--------|-------------|
| Conversation Memory | ✅ | Redis-backed chat history |
| PostgreSQL Persistence | ✅ | Users, executions, audit logs |
| JWT Authentication | ✅ | Secure token-based auth |
| Distributed Caching | ✅ | Redis pool across workers |
| Queue Execution | ✅ | Celery async task processing |
| Prometheus Metrics | ✅ | Full observability stack |
| Multi-Model Pipeline | ✅ | Chain models together |
| Docker Compose | ✅ | One-command full stack |
| Kubernetes | ✅ | Production deployment manifests |

## Quick Start

### Option 1: Docker Compose (Recommended)
```bash
cd ritual_marketplace_scaffold
docker-compose up -d
# Open: http://localhost:8000
# Grafana: http://localhost:3001 (admin/admin)
```

### Option 2: Local Development
```bash
cd ritual_marketplace_scaffold
pip install -r requirements.txt
python run_server.py
# Open: http://localhost:8000
```

### Option 3: Full Stack with Ollama
```bash
docker-compose --profile with-ollama up -d
```

## Deployment Options

### Docker Compose
```bash
# Basic stack
docker-compose up -d

# With Celery workers
docker-compose --profile with-celery up -d

# Full stack
docker-compose --profile with-ollama --profile with-celery up -d

# Scale API
docker-compose up -d --scale api=3
```

### Kubernetes
```bash
kubectl apply -f k8s/deployment.yaml
```

## API Reference

### Authentication
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/auth/login` | POST | Login, returns JWT tokens |
| `/api/auth/refresh` | POST | Refresh access token |
| `/api/auth/me` | GET | Get current user |

### Registry
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/registry/search` | GET | Search models |
| `/api/registry/model/{id}` | GET | Get model details |
| `/api/registry/tags` | GET | List available tags |

### Execution
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/zoo/run` | POST | Execute model (async) |
| `/api/zoo/stream` | GET | SSE streaming execution |
| `/api/zoo/cancel/{id}` | POST | Cancel execution |
| `/api/zoo/prewarm` | POST | Prewarm models |

### Queue (Celery)
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/queue/submit` | POST | Submit async task |
| `/api/queue/status/{id}` | GET | Get task status |
| `/api/queue/result/{id}` | GET | Get task result |

### Pipeline
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/pipeline/run` | POST | Run a pipeline |
| `/api/pipeline/list` | GET | List available pipelines |

### Conversations
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/conversations` | GET | List user conversations |
| `/api/conversations/{id}` | GET | Get conversation |
| `/api/conversations/{id}/messages` | POST | Add message |

### Observability
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/metrics` | GET | Prometheus metrics |
| `/api/health` | GET | System health |
| `/api/pool/status` | GET | Model pool stats |
| `/api/acp/status` | GET | ACP audit logs |

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `REDIS_URL` | Redis connection | redis://localhost:6379 |
| `POSTGRES_URL` | PostgreSQL connection | postgresql://... |
| `SECRET_KEY` | JWT secret | auto-generated |
| `OLLAMA_BASE_URL` | Ollama server | localhost:11434 |
| `VLLM_BASE_URL` | vLLM server | localhost:8000 |
| `OPENAI_BASE_URL` | OpenAI API | - |

## Prometheus Metrics

- `zoo_executions_total` - Total model executions
- `zoo_execution_latency_seconds` - Execution latency histogram
- `zoo_cache_hits_total` / `zoo_cache_misses_total` - Cache performance
- `zoo_api_requests_total` - API request counts
- `zoo_ws_active_connections` - Active WebSocket connections
- `zoo_active_executions` - Currently running executions

## Demo Credentials

| User | Password | Tier |
|------|----------|------|
| admin | admin123 | Admin |
| demo | demo123 | Standard |
| premium | premium123 | Premium |

## File Structure

```
ritual_marketplace_scaffold/
├── src/backend/
│   ├── api/routes.py           # API endpoints
│   ├── registry/models.py      # Model registry
│   ├── server.py              # FastAPI app
│   └── services/
│       ├── llm_backends.py     # Ollama/vLLM adapters
│       ├── model_pool.py       # Prewarm pool
│       ├── websocket_handler.py # WebSocket streaming
│       ├── acp_enforcer.py     # Permission enforcement
│       ├── conversation_store.py # Redis chat history
│       ├── database.py         # PostgreSQL models
│       ├── auth.py             # JWT authentication
│       ├── distributed_pool.py  # Redis distributed cache
│       ├── execution_queue.py  # Celery queue
│       ├── celery_tasks.py     # Background tasks
│       ├── metrics.py          # Prometheus metrics
│       └── pipeline.py         # Multi-model orchestration
├── k8s/deployment.yaml         # Kubernetes manifests
├── docker-compose.yaml         # Full stack compose
├── Dockerfile                  # Container image
├── init.sql                    # PostgreSQL schema
├── requirements.txt            # Python dependencies
└── EXPANSION_ROADMAP.md       # Future features
```

## Production Checklist

- [ ] Set strong `SECRET_KEY` environment variable
- [ ] Configure Redis with persistence (`appendonly yes`)
- [ ] Set up PostgreSQL with connection pooling
- [ ] Enable TLS/SSL for all endpoints
- [ ] Configure rate limiting per user tier
- [ ] Set up monitoring dashboards in Grafana
- [ ] Configure alerting rules in Prometheus
- [ ] Set up log aggregation
- [ ] Enable HTTPS (use reverse proxy)
- [ ] Configure backup strategy for PostgreSQL
