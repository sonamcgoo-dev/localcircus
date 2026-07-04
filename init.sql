-- RITUAL Marketplace - PostgreSQL Initialization
-- Creates tables and indexes for the persistence layer

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Users table
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    username VARCHAR(255) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE,
    hashed_password VARCHAR(255) NOT NULL,
    tier VARCHAR(50) DEFAULT 'standard',
    is_active BOOLEAN DEFAULT true,
    is_verified BOOLEAN DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP WITH TIME ZONE
);

CREATE INDEX IF NOT EXISTS ix_users_username ON users(username);
CREATE INDEX IF NOT EXISTS ix_users_email ON users(email);
CREATE INDEX IF NOT EXISTS ix_users_tier ON users(tier);

-- Executions table
CREATE TABLE IF NOT EXISTS executions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    model_id VARCHAR(255) NOT NULL,
    input_data TEXT NOT NULL,
    output_data TEXT,
    input_tokens INTEGER DEFAULT 0,
    output_tokens INTEGER DEFAULT 0,
    latency_ms INTEGER DEFAULT 0,
    status VARCHAR(50) DEFAULT 'completed',
    error_message TEXT,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_executions_user_id ON executions(user_id);
CREATE INDEX IF NOT EXISTS ix_executions_model_id ON executions(model_id);
CREATE INDEX IF NOT EXISTS ix_executions_created_at ON executions(created_at DESC);
CREATE INDEX IF NOT EXISTS ix_executions_user_created ON executions(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_executions_model_created ON executions(model_id, created_at DESC);

-- Conversations table
CREATE TABLE IF NOT EXISTS conversations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    title VARCHAR(255) DEFAULT 'New Chat',
    messages JSONB DEFAULT '[]',
    metadata JSONB DEFAULT '{}',
    is_archived BOOLEAN DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_conversations_user_id ON conversations(user_id);
CREATE INDEX IF NOT EXISTS ix_conversations_updated_at ON conversations(updated_at DESC);
CREATE INDEX IF NOT EXISTS ix_conversations_user_archived ON conversations(user_id, is_archived);

-- Audit logs table
CREATE TABLE IF NOT EXISTS audit_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    action VARCHAR(100) NOT NULL,
    resource VARCHAR(255),
    resource_id UUID,
    details JSONB DEFAULT '{}',
    ip_address VARCHAR(45),
    user_agent TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_audit_logs_user_id ON audit_logs(user_id);
CREATE INDEX IF NOT EXISTS ix_audit_logs_action ON audit_logs(action);
CREATE INDEX IF NOT EXISTS ix_audit_logs_created_at ON audit_logs(created_at DESC);
CREATE INDEX IF NOT EXISTS ix_audit_logs_user_action ON audit_logs(user_id, action);

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Triggers for updated_at
CREATE TRIGGER update_users_updated_at
    BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_conversations_updated_at
    BEFORE UPDATE ON conversations
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Insert demo users
INSERT INTO users (id, username, email, hashed_password, tier)
VALUES 
    ('00000000-0000-0000-0000-000000000001', 'admin', 'admin@ritual.ai', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewY5lWkJ6fClOH0W', 'admin'),
    ('00000000-0000-0000-0000-000000000002', 'demo', 'demo@ritual.ai', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewY5lWkJ6fClOH0W', 'standard'),
    ('00000000-0000-0000-0000-000000000003', 'premium', 'premium@ritual.ai', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewY5lWkJ6fClOH0W', 'premium')
ON CONFLICT (username) DO NOTHING;

-- Create views for common queries
CREATE OR REPLACE VIEW execution_stats AS
SELECT 
    user_id,
    model_id,
    COUNT(*) as total_executions,
    SUM(output_tokens) as total_tokens,
    AVG(latency_ms)::INTEGER as avg_latency_ms,
    MIN(created_at) as first_execution,
    MAX(created_at) as last_execution
FROM executions
GROUP BY user_id, model_id;

CREATE OR REPLACE VIEW daily_execution_counts AS
SELECT 
    DATE(created_at) as date,
    COUNT(*) as executions,
    COUNT(DISTINCT user_id) as unique_users,
    SUM(output_tokens) as total_tokens
FROM executions
GROUP BY DATE(created_at)
ORDER BY date DESC;
