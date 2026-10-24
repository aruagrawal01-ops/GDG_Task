CREATE TABLE IF NOT EXISTS readings (
    id        BIGSERIAL PRIMARY KEY,
    sensor_id TEXT NOT NULL,
    type      TEXT NOT NULL,
    value     DOUBLE PRECISION NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS readings_sensor_type_time ON readings (sensor_id, type, timestamp);

CREATE TABLE IF NOT EXISTS rules (
    id             SERIAL PRIMARY KEY,
    name           TEXT NOT NULL,
    type           TEXT NOT NULL,
    operator       TEXT NOT NULL CHECK (operator IN ('>', '>=', '<', '<=')),
    threshold      DOUBLE PRECISION NOT NULL,
    window_minutes INT CHECK (window_minutes > 0),
    enabled        BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS alerts (
    id         BIGSERIAL PRIMARY KEY,
    rule_id    INT NOT NULL REFERENCES rules (id),
    sensor_id  TEXT NOT NULL,
    value      DOUBLE PRECISION NOT NULL,
    status     TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'acknowledged', 'resolved')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS one_active_alert ON alerts (rule_id, sensor_id) WHERE status <> 'resolved';
