CREATE TABLE IF NOT EXISTS readings (
    id          BIGSERIAL PRIMARY KEY,
    sensor_id   TEXT NOT NULL,
    type        TEXT NOT NULL,
    value       DOUBLE PRECISION NOT NULL,
    ts          TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS readings_sensor_ts ON readings (sensor_id, ts DESC);
CREATE INDEX IF NOT EXISTS readings_type_ts ON readings (type, ts DESC);

CREATE TABLE IF NOT EXISTS alert_rules (
    id             SERIAL PRIMARY KEY,
    name           TEXT NOT NULL,
    sensor_type    TEXT NOT NULL,
    sensor_id      TEXT,
    kind           TEXT NOT NULL CHECK (kind IN ('single', 'window')),
    operator       TEXT NOT NULL CHECK (operator IN ('>', '>=', '<', '<=')),
    threshold      DOUBLE PRECISION NOT NULL,
    window_seconds INT CHECK (window_seconds > 0),
    aggregation    TEXT CHECK (aggregation IN ('avg', 'min', 'max', 'sum', 'count')),
    min_samples    INT NOT NULL DEFAULT 1 CHECK (min_samples > 0),
    enabled        BOOLEAN NOT NULL DEFAULT TRUE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (kind = 'single' OR (window_seconds IS NOT NULL AND aggregation IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS alerts (
    id                BIGSERIAL PRIMARY KEY,
    rule_id           INT NOT NULL REFERENCES alert_rules (id),
    sensor_id         TEXT NOT NULL,
    status            TEXT NOT NULL DEFAULT 'open'
                      CHECK (status IN ('open', 'acknowledged', 'resolved')),
    message           TEXT NOT NULL,
    triggered_value   DOUBLE PRECISION NOT NULL,
    last_value        DOUBLE PRECISION NOT NULL,
    trigger_count     INT NOT NULL DEFAULT 1,
    reading_ts        TIMESTAMPTZ NOT NULL,
    last_reading_ts   TIMESTAMPTZ NOT NULL,
    triggered_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_triggered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    acknowledged_at   TIMESTAMPTZ,
    resolved_at       TIMESTAMPTZ
);
CREATE UNIQUE INDEX IF NOT EXISTS one_active_alert
    ON alerts (rule_id, sensor_id) WHERE status IN ('open', 'acknowledged');
CREATE INDEX IF NOT EXISTS alerts_status_id ON alerts (status, id DESC);
