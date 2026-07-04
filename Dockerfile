# RITUAL Marketplace - Production Dockerfile
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install additional production dependencies
RUN pip install --no-cache-dir \
    redis \
    asyncpg \
    sqlalchemy \
    pyjwt \
    bcrypt \
    prometheus-client \
    celery[redis]

# Copy application code
COPY src/ ./src/
COPY run_server.py .

# Create non-root user
RUN useradd -m -u 1000 apiuser && \
    chown -R apiuser:apiuser /app
USER apiuser

# Environment variables
ENV PYTHONUNBUFFERED=1
ENV HOST=0.0.0.0
ENV PORT=8000

# Expose ports
EXPOSE 8000 9090

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Run the application
CMD ["python", "run_server.py"]
