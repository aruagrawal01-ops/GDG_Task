from psycopg2.errors import UniqueViolation
from psycopg2.extras import Json, RealDictCursor

CLAIM_SEAT = """
UPDATE workshops SET seats_taken = seats_taken + 1
WHERE id = %s AND status = 'active' AND (capacity IS NULL OR seats_taken < capacity)
RETURNING title, starts_at
"""

INSERT_CONFIRMATION = """
INSERT INTO outbox (kind, registration_id, workshop_id, member_id, payload, idempotency_key)
VALUES ('confirmation', %s, %s, %s, %s, %s)
RETURNING id
"""

INSERT_CANCELLATIONS = """
INSERT INTO outbox (kind, registration_id, workshop_id, member_id, payload, idempotency_key, depends_on)
SELECT 'cancellation', r.id, r.workshop_id, r.member_id,
       jsonb_build_object(
           'event', 'workshop_cancelled', 'registrationId', r.id, 'workshopId', w.id,
           'workshopTitle', w.title, 'startsAt', w.starts_at, 'memberId', m.id,
           'memberName', m.name, 'email', m.email, 'occurredAt', w.cancelled_at),
       'cancellation:' || r.id,
       c.id
FROM registrations r
JOIN workshops w ON w.id = r.workshop_id
JOIN members m ON m.id = r.member_id
LEFT JOIN outbox c ON c.idempotency_key = 'confirmation:' || r.id
WHERE r.workshop_id = %s
ON CONFLICT (idempotency_key) DO NOTHING
"""


class RegistrationError(Exception):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status
        self.message = message


def register(db, workshop_id, member_id):
    """Save the registration and its confirmation intent in one transaction (transactional outbox)."""
    with db.connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT id, name, email FROM members WHERE id = %s", (member_id,))
        member = cur.fetchone()
        if member is None:
            raise RegistrationError(404, "member not found")
        cur.execute("SELECT id FROM workshops WHERE id = %s", (workshop_id,))
        if cur.fetchone() is None:
            raise RegistrationError(404, "workshop not found")

        try:
            cur.execute(
                "INSERT INTO registrations (workshop_id, member_id) VALUES (%s, %s) RETURNING id, created_at",
                (workshop_id, member_id),
            )
        except UniqueViolation:
            raise RegistrationError(409, "member is already registered for this workshop") from None
        registration = cur.fetchone()

        cur.execute(CLAIM_SEAT, (workshop_id,))
        workshop = cur.fetchone()
        if workshop is None:
            cur.execute("SELECT status FROM workshops WHERE id = %s", (workshop_id,))
            status = cur.fetchone()["status"]
            raise RegistrationError(409, "workshop is cancelled" if status == "cancelled" else "workshop is full")

        payload = {
            "event": "registration_confirmed",
            "registrationId": registration["id"],
            "workshopId": workshop_id,
            "workshopTitle": workshop["title"],
            "startsAt": workshop["starts_at"].isoformat() if workshop["starts_at"] else None,
            "memberId": member["id"],
            "memberName": member["name"],
            "email": member["email"],
            "occurredAt": registration["created_at"].isoformat(),
        }
        cur.execute(INSERT_CONFIRMATION, (registration["id"], workshop_id, member_id, Json(payload),
                                          f"confirmation:{registration['id']}"))
        return {
            "registrationId": registration["id"],
            "workshopId": workshop_id,
            "memberId": member_id,
            "status": "registered",
            "confirmation": "pending",
            "createdAt": registration["created_at"].isoformat(),
        }


def cancel(db, workshop_id):
    """Cancel the workshop and queue one notification per registration in one transaction."""
    with db.connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "UPDATE workshops SET status = 'cancelled', cancelled_at = now() "
            "WHERE id = %s AND status = 'active' RETURNING *",
            (workshop_id,),
        )
        workshop = cur.fetchone()
        if workshop is None:
            cur.execute("SELECT * FROM workshops WHERE id = %s", (workshop_id,))
            existing = cur.fetchone()
            if existing is None:
                raise RegistrationError(404, "workshop not found")
            return existing, 0, True
        cur.execute(INSERT_CANCELLATIONS, (workshop_id,))
        return workshop, cur.rowcount, False
