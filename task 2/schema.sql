CREATE TABLE IF NOT EXISTS workshops (
    id     SERIAL PRIMARY KEY,
    title  TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'cancelled'))
);

CREATE TABLE IF NOT EXISTS registrations (
    id          SERIAL PRIMARY KEY,
    workshop_id INT NOT NULL REFERENCES workshops (id),
    member_id   TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (workshop_id, member_id)
);

CREATE TABLE IF NOT EXISTS notifications (
    id              SERIAL PRIMARY KEY,
    registration_id INT NOT NULL REFERENCES registrations (id),
    kind            TEXT NOT NULL CHECK (kind IN ('confirmation', 'cancellation')),
    status          TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'sent')),
    attempts        INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error      TEXT,
    UNIQUE (registration_id, kind)
);
