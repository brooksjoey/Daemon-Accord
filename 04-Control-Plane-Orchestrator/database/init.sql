-- Initialize database schema

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Job results table
CREATE TABLE IF NOT EXISTS job_results (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    workflow_name TEXT NOT NULL,
    workflow_version TEXT DEFAULT '1.0.0',
    results JSONB NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'executing', 'completed', 'failed', 'cancelled', 'timeout')),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    execution_time INTERVAL GENERATED ALWAYS AS (completed_at - started_at) STORED,
    error_message TEXT,
    user_id TEXT,
    priority INTEGER DEFAULT 1 CHECK (priority BETWEEN 1 AND 10),
    tags JSONB DEFAULT '[]'::jsonb,
    metadata JSONB DEFAULT '{}'::jsonb
);

-- Artifacts table
CREATE TABLE IF NOT EXISTS artifacts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_id UUID REFERENCES job_results(id) ON DELETE CASCADE,
    artifact_type TEXT NOT NULL,
    storage_key TEXT NOT NULL,
    storage_url TEXT NOT NULL,
    content_type TEXT,
    file_size BIGINT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    metadata JSONB DEFAULT '{}'::jsonb
);

-- Configurations table
CREATE TABLE IF NOT EXISTS configurations (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    config JSONB NOT NULL,
    version TEXT NOT NULL,
    hash TEXT NOT NULL,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by TEXT DEFAULT 'system',
    UNIQUE(name, version)
);

-- Config changes table
CREATE TABLE IF NOT EXISTS config_changes (
    id SERIAL PRIMARY KEY,
    config_name TEXT NOT NULL,
    version TEXT NOT NULL,
    changed_by TEXT NOT NULL,
    changed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    change_type TEXT DEFAULT 'update',
    previous_version TEXT,
    notes TEXT
);

-- Rate limit logs
CREATE TABLE IF NOT EXISTS rate_limit_logs (
    id SERIAL PRIMARY KEY,
    identifier TEXT NOT NULL,
    window TEXT NOT NULL,
    request_count INTEGER NOT NULL,
    limit_count INTEGER NOT NULL,
    is_blocked BOOLEAN DEFAULT false,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    ip_address INET,
    user_agent TEXT,
    endpoint TEXT
);

-- System metrics
CREATE TABLE IF NOT EXISTS system_metrics (
    id SERIAL PRIMARY KEY,
    metric_type TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    metric_value DOUBLE PRECISION NOT NULL,
    labels JSONB DEFAULT '{}'::jsonb,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Users table
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email TEXT UNIQUE NOT NULL,
    api_key TEXT UNIQUE NOT NULL,
    plan TEXT DEFAULT 'free' CHECK (plan IN ('free', 'basic', 'pro', 'enterprise', 'admin')),
    rate_limit_config JSONB DEFAULT '{"minute": 5, "hour": 50}'::jsonb,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_login TIMESTAMP WITH TIME ZONE,
    metadata JSONB DEFAULT '{}'::jsonb
);

-- Audit logs
CREATE TABLE IF NOT EXISTS audit_logs (
    id SERIAL PRIMARY KEY,
    user_id UUID REFERENCES users(id),
    action TEXT NOT NULL,
    resource_type TEXT,
    resource_id TEXT,
    details JSONB DEFAULT '{}'::jsonb,
    ip_address INET,
    user_agent TEXT,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Request logs
CREATE TABLE IF NOT EXISTS request_logs (
    id SERIAL PRIMARY KEY,
    method TEXT NOT NULL,
    path TEXT NOT NULL,
    status_code INTEGER NOT NULL,
    duration DOUBLE PRECISION NOT NULL,
    error TEXT,
    ip_address INET,
    user_agent TEXT,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes
CREATE INDEX idx_job_results_created_at ON job_results(created_at DESC);
CREATE INDEX idx_job_results_status ON job_results(status);
CREATE INDEX idx_job_results_user_id ON job_results(user_id);
CREATE INDEX idx_job_results_priority ON job_results(priority);

CREATE INDEX idx_artifacts_job_id ON artifacts(job_id);
CREATE INDEX idx_artifacts_artifact_type ON artifacts(artifact_type);
CREATE INDEX idx_artifacts_created_at ON artifacts(created_at DESC);

CREATE INDEX idx_configurations_name ON configurations(name);
CREATE INDEX idx_configurations_active ON configurations(is_active) WHERE is_active = true;

CREATE INDEX idx_config_changes_config_name ON config_changes(config_name);
CREATE INDEX idx_config_changes_changed_at ON config_changes(changed_at DESC);

CREATE INDEX idx_rate_limit_logs_identifier ON rate_limit_logs(identifier);
CREATE INDEX idx_rate_limit_logs_timestamp ON rate_limit_logs(timestamp DESC);

CREATE INDEX idx_system_metrics_timestamp ON system_metrics(timestamp DESC);
CREATE INDEX idx_system_metrics_type_name ON system_metrics(metric_type, metric_name);

CREATE INDEX idx_users_api_key ON users(api_key) WHERE is_active = true;
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_plan ON users(plan);

CREATE INDEX idx_audit_logs_timestamp ON audit_logs(timestamp DESC);
CREATE INDEX idx_audit_logs_user_id ON audit_logs(user_id);
CREATE INDEX idx_audit_logs_action ON audit_logs(action);

CREATE INDEX idx_request_logs_timestamp ON request_logs(timestamp DESC);
CREATE INDEX idx_request_logs_method_path ON request_logs(method, path);
CREATE INDEX idx_request_logs_status_code ON request_logs(status_code);

-- Insert default admin user (change password in production)
INSERT INTO users (email, api_key, plan, rate_limit_config) 
VALUES (
    'admin@example.com',
    'admin-default-key-change-in-production',
    'admin',
    '{"minute": 1000, "hour": 10000}'::jsonb
) ON CONFLICT (email) DO NOTHING;

-- Insert default configurations
INSERT INTO configurations (name, config, version, hash, is_active) VALUES
(
    'rate_limits',
    '{
        "default": {"minute": 60, "hour": 1000},
        "free": {"minute": 5, "hour": 50},
        "basic": {"minute": 30, "hour": 500},
        "pro": {"minute": 100, "hour": 5000},
        "enterprise": {"minute": 500, "hour": 50000}
    }',
    'v1.0.0',
    'hash1',
    true
),
(
    'browser_pool',
    '{
        "max_browsers": 10,
        "user_agents": [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
        ],
        "headless": true,
        "stealth_mode": true,
        "viewport": {"width": 1920, "height": 1080}
    }',
    'v1.0.0',
    'hash2',
    true
),
(
    'workflow_defaults',
    '{
        "timeout": 300,
        "retry_count": 3,
        "priority": 1,
        "capture_artifacts": true,
        "rate_limit_delay": 2
    }',
    'v1.0.0',
    'hash3',
    true
) ON CONFLICT (name, version) DO NOTHING;
