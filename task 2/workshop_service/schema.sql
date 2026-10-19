CREATE TABLE IF NOT EXISTS workshops (
    id           SERIAL PRIMARY KEY,
    title        TEXT NOT NULL,
    starts_at    TIMESTAMPTZ,
    capacity     INT CHECK (capacity > 0),
    seats_taken  INT NOT NULL DEFAULT 0,
    status       TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'cancelled')),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    cancelled_at TIMESTAMPTZ,
    CHECK (capacity IS NULL OR seats_taken <= capacity)
);

CREATE TABLE IF NOT EXISTS members (
    id         SERIAL PRIMARY KEY,
    name       TEXT NOT NULL,
    email      TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS registrations (
    id          BIGSERIAL PRIMARY KEY,
    workshop_id INT NOT NULL REFERENCES workshops (id),
    member_id   INT NOT NULL REFERENCES members (id),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (workshop_id, member_id)
);

CREATE TABLE IF NOT EXISTS outbox (
    id              BIGSERIAL PRIMARY KEY,
    kind            TEXT NOT NULL CHECK (kind IN ('confirmation', 'cancellation')),
    registration_id BIGINT NOT NULL REFERENCES registrations (id),
    workshop_id     INT NOT NULL,
    member_id       INT NOT NULL,
    payload         JSONB NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    depends_on      BIGINT REFERENCES outbox (id),
    status          TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'sent', 'dead')),
    attempts        INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_until    TIMESTAMPTZ,
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at         TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS outbox_due ON outbox (next_attempt_at) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS outbox_workshop ON outbox (workshop_id, id);
