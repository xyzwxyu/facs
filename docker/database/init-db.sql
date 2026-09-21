-- FACS PostgreSQL schema. Keep this aligned with src/core/db.py.

CREATE TABLE IF NOT EXISTS devices (
    id SERIAL PRIMARY KEY,
    device_id VARCHAR(255) UNIQUE NOT NULL,
    serial_number VARCHAR(128) UNIQUE NOT NULL,
    manufacturer VARCHAR(128),
    oui VARCHAR(6) NOT NULL,
    product_class VARCHAR(128),
    software_version VARCHAR(128),
    hardware_version VARCHAR(128),
    ip_address VARCHAR(45),
    connection_request_url TEXT,
    last_inform TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS device_parameters (
    id SERIAL PRIMARY KEY,
    device_id VARCHAR(255) NOT NULL REFERENCES devices(device_id) ON DELETE CASCADE,
    path VARCHAR(1024) NOT NULL,
    value TEXT,
    value_type VARCHAR(64) NOT NULL DEFAULT 'xsd:string',
    writable BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_device_parameter_path UNIQUE (device_id, path)
);

CREATE TABLE IF NOT EXISTS task_queue (
    id SERIAL PRIMARY KEY,
    task_id VARCHAR(64) UNIQUE NOT NULL,
    device_id VARCHAR(255) NOT NULL REFERENCES devices(device_id) ON DELETE CASCADE,
    command_type VARCHAR(64) NOT NULL,
    command JSONB,
    command_key VARCHAR(64),
    status VARCHAR(32) NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'sent', 'completed', 'failed')),
    retry_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    sent_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS files (
    id SERIAL PRIMARY KEY,
    filename VARCHAR(255) NOT NULL,
    
    -- TR-069 spec maps file type to string(64)
    file_type VARCHAR(64) NOT NULL, 
    content BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT chk_tr069_file_type CHECK (file_type IN (
        '1 Firmware Upgrade Image',
        '2 Web Content',
        '3 Vendor Configuration File'
    ))
);

CREATE INDEX IF NOT EXISTS ix_devices_device_id ON devices(device_id);
CREATE INDEX IF NOT EXISTS ix_devices_serial_number ON devices(serial_number);
CREATE INDEX IF NOT EXISTS idx_device_parameters_device_path ON device_parameters(device_id, path);
CREATE INDEX IF NOT EXISTS idx_task_queue_device_status_created ON task_queue(device_id, status, created_at);
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO facs;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO facs;

