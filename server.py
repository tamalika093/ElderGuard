"""
ElderGuard - simple, persistent, role-based remote care backend.

Design goals for this version:
  * SQLite is the source of truth (no more in-memory-only state).
  * Real accounts, sessions and explicit caregiver consent.
  * No hardcoded patient / emergency-contact data in application code.
  * SOS never claims an ambulance was dispatched. Registered facility desks
    receive a request record they may acknowledge or decline only.
  * Per-user SSE so medical data is not broadcast to everyone.
"""

from __future__ import annotations

import json
import math
import os
import queue
import secrets
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta
from functools import wraps

from flask import (
    Flask,
    Response,
    g,
    jsonify,
    request,
    send_from_directory,
    session,
)
from werkzeug.security import check_password_hash, generate_password_hash

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_FILE = os.path.join(DATA_DIR, "elderguard.db")
STATE_FILE = os.path.join(DATA_DIR, "state.json")  # human-readable snapshot, not source of truth
SECRET_FILE = os.path.join(DATA_DIR, "secret.key")
STATIC_DIR = os.path.join(BASE_DIR, "static")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(STATIC_DIR, exist_ok=True)


def load_secret() -> str:
    if os.path.exists(SECRET_FILE):
        with open(SECRET_FILE, "r", encoding="utf-8") as fh:
            value = fh.read().strip()
            if value:
                return value
    value = secrets.token_hex(32)
    with open(SECRET_FILE, "w", encoding="utf-8") as fh:
        fh.write(value)
    return value


app = Flask(__name__, static_folder=STATIC_DIR, static_url_path="")
app.secret_key = load_secret()
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=os.environ.get("ELDERGUARD_HTTPS", "").lower() == "true",
    PERMANENT_SESSION_LIFETIME=timedelta(days=7),
    MAX_CONTENT_LENGTH=1 * 1024 * 1024,
)

db_lock = threading.RLock()


@contextmanager
def db():
    conn = sqlite3.connect(DB_FILE, timeout=10, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        with db_lock:
            yield conn
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def row_dict(row) -> dict | None:
    return dict(row) if row is not None else None


SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    phone TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL CHECK(role IN ('patient','caregiver','provider')),
    city TEXT NOT NULL DEFAULT '',
    lat REAL,
    lng REAL,
    facility_id TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS facilities (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    address TEXT NOT NULL DEFAULT '',
    city TEXT NOT NULL DEFAULT '',
    lat REAL,
    lng REAL,
    phone TEXT NOT NULL DEFAULT '',
    verified INTEGER NOT NULL DEFAULT 0,
    integration_status TEXT NOT NULL DEFAULT 'none',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS invite_codes (
    code TEXT PRIMARY KEY,
    patient_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    used INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS relationships (
    id TEXT PRIMARY KEY,
    patient_id TEXT NOT NULL,
    caregiver_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('pending','active','declined')),
    can_manage_meds INTEGER NOT NULL DEFAULT 1,
    can_view_health INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(patient_id, caregiver_id)
);

CREATE TABLE IF NOT EXISTS medical_profiles (
    user_id TEXT PRIMARY KEY,
    blood_group TEXT NOT NULL DEFAULT '',
    allergies TEXT NOT NULL DEFAULT '',
    conditions TEXT NOT NULL DEFAULT '',
    doctor_name TEXT NOT NULL DEFAULT '',
    doctor_phone TEXT NOT NULL DEFAULT '',
    share_medical INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS emergency_contacts (
    id TEXT PRIMARY KEY,
    patient_id TEXT NOT NULL,
    name TEXT NOT NULL,
    phone TEXT NOT NULL,
    relation TEXT NOT NULL DEFAULT '',
    verified INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS medications (
    id TEXT PRIMARY KEY,
    patient_id TEXT NOT NULL,
    name TEXT NOT NULL,
    dose TEXT NOT NULL DEFAULT '',
    condition TEXT NOT NULL DEFAULT '',
    schedule_time TEXT NOT NULL,
    instructions TEXT NOT NULL DEFAULT '',
    created_by TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS medication_events (
    id TEXT PRIMARY KEY,
    medication_id TEXT NOT NULL,
    patient_id TEXT NOT NULL,
    due_at TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('pending','taken','snoozed','missed')),
    acknowledged_at TEXT,
    acknowledged_by TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS alerts (
    id TEXT PRIMARY KEY,
    patient_id TEXT NOT NULL,
    sender_id TEXT NOT NULL,
    alert_type TEXT NOT NULL,
    message TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'sent',
    created_at TEXT NOT NULL,
    acknowledged_at TEXT,
    acknowledged_by TEXT
);

CREATE TABLE IF NOT EXISTS sos_events (
    id TEXT PRIMARY KEY,
    patient_id TEXT NOT NULL,
    created_by TEXT NOT NULL,
    lat REAL,
    lng REAL,
    accuracy REAL,
    location_source TEXT NOT NULL,
    notes TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL CHECK(status IN ('open','acknowledged','resolved','cancelled')),
    created_at TEXT NOT NULL,
    resolved_at TEXT
);

CREATE TABLE IF NOT EXISTS deliveries (
    id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL CHECK(event_type IN ('alert','sos')),
    event_id TEXT NOT NULL,
    channel TEXT NOT NULL,
    recipient_id TEXT,
    recipient_label TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('pending','sent','delivered','acknowledged','failed','sandbox')),
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS devices (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    token TEXT NOT NULL,
    platform TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_logs (
    id TEXT PRIMARY KEY,
    patient_id TEXT,
    actor_id TEXT,
    action TEXT NOT NULL,
    detail TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS notifications (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    type TEXT NOT NULL,
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    data TEXT NOT NULL DEFAULT '{}',
    read INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_notifications_user ON notifications(user_id, read, created_at);
CREATE INDEX IF NOT EXISTS idx_meds_patient ON medications(patient_id);
CREATE INDEX IF NOT EXISTS idx_events_patient ON medication_events(patient_id);
CREATE INDEX IF NOT EXISTS idx_alerts_patient ON alerts(patient_id);
CREATE INDEX IF NOT EXISTS idx_sos_patient ON sos_events(patient_id);
CREATE INDEX IF NOT EXISTS idx_deliveries_event ON deliveries(event_type, event_id);
CREATE INDEX IF NOT EXISTS idx_deliveries_recipient ON deliveries(event_type, recipient_id);
CREATE INDEX IF NOT EXISTS idx_audit_patient ON audit_logs(patient_id);
"""


def ensure_column(conn, table: str, column: str, definition: str) -> None:
    """Add a column to an older database that predates it."""
    cols = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}
    if cols and column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def migrate_nullable_coords(conn, table: str, create_sql: str, columns: str) -> None:
    """Allow older databases with NOT NULL coordinates to upgrade in place."""
    cols = {row["name"]: dict(row) for row in conn.execute(f"PRAGMA table_info({table})")}
    if not cols:
        return
    if cols.get("lat", {}).get("notnull") or cols.get("lng", {}).get("notnull"):
        conn.execute(f"ALTER TABLE {table} RENAME TO {table}_old")
        conn.execute(create_sql)
        conn.execute(f"INSERT INTO {table} ({columns}) SELECT {columns} FROM {table}_old")
        conn.execute(f"DROP TABLE {table}_old")


def init_db() -> None:
    with db() as conn:
        conn.executescript(SCHEMA)
        ensure_column(conn, "facilities", "city", "TEXT NOT NULL DEFAULT ''")
        migrate_nullable_coords(
            conn,
            "facilities",
            "CREATE TABLE facilities ("
            "id TEXT PRIMARY KEY, name TEXT NOT NULL, address TEXT NOT NULL DEFAULT '', "
            "city TEXT NOT NULL DEFAULT '', lat REAL, lng REAL, phone TEXT NOT NULL DEFAULT '', "
            "verified INTEGER NOT NULL DEFAULT 0, "
            "integration_status TEXT NOT NULL DEFAULT 'none', created_at TEXT NOT NULL)",
            "id,name,address,city,lat,lng,phone,verified,integration_status,created_at",
        )
        migrate_nullable_coords(
            conn,
            "sos_events",
            "CREATE TABLE sos_events ("
            "id TEXT PRIMARY KEY, patient_id TEXT NOT NULL, created_by TEXT NOT NULL, "
            "lat REAL, lng REAL, accuracy REAL, location_source TEXT NOT NULL, "
            "notes TEXT NOT NULL DEFAULT '', "
            "status TEXT NOT NULL CHECK(status IN ('open','acknowledged','resolved','cancelled')), "
            "created_at TEXT NOT NULL, resolved_at TEXT)",
            "id,patient_id,created_by,lat,lng,accuracy,location_source,notes,status,created_at,resolved_at",
        )
        # Accounts created before the provider desk existed had an inactive
        # facility row. Activate those desks so they are usable after upgrade.
        conn.execute(
            "UPDATE facilities SET city = COALESCE(NULLIF(city, ''), ("
            "SELECT city FROM users WHERE users.facility_id=facilities.id "
            "AND role='provider' LIMIT 1)) WHERE city='' AND EXISTS ("
            "SELECT 1 FROM users WHERE users.facility_id=facilities.id AND role='provider')"
        )
        conn.execute(
            "UPDATE facilities SET verified=1, integration_status='request_only' "
            "WHERE verified=0 AND EXISTS ("
            "SELECT 1 FROM users WHERE users.facility_id=facilities.id AND role='provider')"
        )


init_db()

# ---------------------------------------------------------------------------
# Realtime delivery (per user, not global)
# ---------------------------------------------------------------------------

user_queues: dict[str, list[queue.Queue]] = {}
queues_lock = threading.Lock()


def emit(user_id: str, event_type: str, data: dict) -> None:
    payload = json.dumps(
        {"type": event_type, "data": data, "timestamp": now_iso()},
        default=str,
    )
    with queues_lock:
        for q in list(user_queues.get(user_id, [])):
            try:
                q.put_nowait(payload)
            except queue.Full:
                pass


def add_log(conn, patient_id, actor_id, action, detail) -> dict:
    entry = {
        "id": new_id("log"),
        "patient_id": patient_id,
        "actor_id": actor_id,
        "action": action,
        "detail": detail,
        "created_at": now_iso(),
    }
    conn.execute(
        "INSERT INTO audit_logs (id,patient_id,actor_id,action,detail,created_at) "
        "VALUES (:id,:patient_id,:actor_id,:action,:detail,:created_at)",
        entry,
    )
    return entry


def write_snapshot() -> None:
    """Optional readable snapshot. The database remains the source of truth."""
    try:
        with db() as conn:
            counts = {
                table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                for table in (
                    "users", "relationships", "medications", "alerts",
                    "sos_events", "deliveries", "audit_logs", "notifications",
                )
            }
        payload = {
            "generated_at": now_iso(),
            "source_of_truth": DB_FILE,
            "counts": counts,
            "note": "Full records are stored in SQLite. This file is a debug snapshot.",
        }
        with open(STATE_FILE, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2)
    except Exception as exc:  # pragma: no cover - snapshot must never break API
        app.logger.warning("Could not write state snapshot: %s", exc)


write_snapshot()

# ---------------------------------------------------------------------------
# Auth, validation and authorization helpers
# ---------------------------------------------------------------------------


def ok(**kwargs):
    return jsonify({"success": True, **kwargs})


def fail(message: str, code: int = 400):
    return jsonify({"success": False, "error": message}), code


def fields(body: dict, required: list[str]) -> str | None:
    for name in required:
        value = body.get(name)
        if value is None or (isinstance(value, str) and not value.strip()):
            return f"Missing required field: {name}"
    return None


def text(value, limit: int = 500) -> str:
    return str(value or "").strip()[:limit]


def valid_email(value: str) -> bool:
    return "@" in value and "." in value.split("@")[-1]


def get_user():
    uid = session.get("uid")
    if not uid:
        return None
    with db() as conn:
        return row_dict(conn.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone())


def login_required(fn):
    @wraps(fn)
    def wrapped(*args, **kwargs):
        user = get_user()
        if not user:
            return fail("Please sign in first.", 401)
        g.user = user
        return fn(*args, **kwargs)

    return wrapped


def role_required(*roles):
    def decorator(fn):
        @wraps(fn)
        @login_required
        def wrapped(*args, **kwargs):
            if g.user["role"] not in roles:
                return fail("Your role cannot perform this action.", 403)
            return fn(*args, **kwargs)

        return wrapped

    return decorator


def stakeholder_ids(conn, patient_id: str) -> list[str]:
    ids = {patient_id}
    for row in conn.execute(
        "SELECT caregiver_id FROM relationships "
        "WHERE patient_id=? AND status='active'", (patient_id,)
    ):
        ids.add(row["caregiver_id"])
    return list(ids)


def can_access(conn, user: dict, patient_id: str, *, manage: bool = False, health: bool = False) -> bool:
    if user["role"] == "provider":
        return False
    if user["id"] == patient_id:
        return True
    rel = conn.execute(
        "SELECT * FROM relationships WHERE patient_id=? AND caregiver_id=? AND status='active'",
        (patient_id, user["id"]),
    ).fetchone()
    if not rel:
        return False
    if manage and not rel["can_manage_meds"]:
        return False
    if health and not rel["can_view_health"]:
        return False
    return True


def access_or_403(conn, patient_id: str, **kwargs):
    if not can_access(conn, g.user, patient_id, **kwargs):
        return fail("You are not authorized for this patient.", 403)
    return None


def safe_patient(conn, patient_id: str):
    return row_dict(conn.execute(
        "SELECT id,name,role,phone,city,lat,lng FROM users WHERE id=? AND role='patient'",
        (patient_id,),
    ).fetchone())


def haversine(lat1, lng1, lat2, lng2) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lng2 - lng1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def create_delivery(conn, event_type, event_id, channel, recipient_id, label, status="pending"):
    entry = {
        "id": new_id("del"),
        "event_type": event_type,
        "event_id": event_id,
        "channel": channel,
        "recipient_id": recipient_id,
        "recipient_label": text(label, 120),
        "status": status,
        "attempts": 0,
        "last_error": None,
        "created_at": now_iso(),
        "updated_at": now_iso(),
    }
    conn.execute(
        "INSERT INTO deliveries (id,event_type,event_id,channel,recipient_id,recipient_label,"
        "status,attempts,last_error,created_at,updated_at) VALUES "
        "(:id,:event_type,:event_id,:channel,:recipient_id,:recipient_label,:status,:attempts,"
        ":last_error,:created_at,:updated_at)",
        entry,
    )
    return entry


def create_notification(conn, user_id: str, kind: str, title: str, body: str, data: dict | None = None) -> dict:
    entry = {
        "id": new_id("ntf"),
        "user_id": user_id,
        "type": kind,
        "title": text(title, 120),
        "body": text(body, 300),
        "data": json.dumps(data or {}),
        "read": 0,
        "created_at": now_iso(),
    }
    conn.execute(
        "INSERT INTO notifications (id,user_id,type,title,body,data,read,created_at) "
        "VALUES (:id,:user_id,:type,:title,:body,:data,:read,:created_at)",
        entry,
    )
    return entry


def ensure_facility_deliveries(conn, facility_id: str | None) -> None:
    """Backfill request records for an existing desk (idempotent)."""
    if not facility_id:
        return
    facility = conn.execute("SELECT name FROM facilities WHERE id=?", (facility_id,)).fetchone()
    if not facility:
        return
    rows = conn.execute(
        "SELECT id FROM sos_events WHERE status IN ('open','acknowledged') "
        "ORDER BY created_at DESC LIMIT 25"
    ).fetchall()
    for row in rows:
        exists = conn.execute(
            "SELECT 1 FROM deliveries WHERE event_type='sos' AND event_id=? AND recipient_id=?",
            (row["id"], facility_id),
        ).fetchone()
        if not exists:
            create_delivery(
                conn, "sos", row["id"], "facility_integration", facility_id,
                f"{facility['name']} (registered request desk)", "sent",
            )


# ---------------------------------------------------------------------------
# Static pages
# ---------------------------------------------------------------------------


@app.route("/")
def index():
    return send_from_directory(STATIC_DIR, "index.html")


@app.route("/sw.js")
def service_worker():
    return send_from_directory(STATIC_DIR, "sw.js", mimetype="application/javascript")


@app.route("/manifest.json")
def manifest():
    return send_from_directory(STATIC_DIR, "manifest.json", mimetype="application/json")


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------


@app.route("/api/auth/register", methods=["POST"])
def register():
    body = request.get_json(silent=True) or {}
    error = fields(body, ["name", "email", "phone", "password", "role"])
    if error:
        return fail(error)
    role = body["role"]
    if role not in ("patient", "caregiver", "provider"):
        return fail("Role must be patient, caregiver or provider.")
    email = text(body["email"], 150).lower()
    if not valid_email(email):
        return fail("Enter a valid email address.")
    if role == "provider" and not text(body.get("facility_name")):
        return fail("Providers must register a participating facility.")

    with db() as conn:
        if conn.execute("SELECT 1 FROM users WHERE email=?", (email,)).fetchone():
            return fail("An account with that email already exists.", 409)

        facility_id = None
        if role == "provider":
            facility_id = new_id("fac")
            facility_city = text(body.get("facility_city") or body.get("city"), 100)
            try:
                lat = float(body["lat"]) if body.get("lat") not in (None, "") else None
                lng = float(body["lng"]) if body.get("lng") not in (None, "") else None
            except (TypeError, ValueError):
                return fail("Facility coordinates must be numbers if provided.")
            # A registered provider account activates an in-app request desk.
            # "verified" here means participating in this prototype, not that an
            # ambulance service or external dispatch integration is connected.
            conn.execute(
                "INSERT INTO facilities (id,name,address,city,lat,lng,phone,verified,integration_status,created_at) "
                "VALUES (?,?,?,?,?,?,?,1,'request_only',?)",
                (
                    facility_id,
                    text(body["facility_name"], 120),
                    text(body.get("facility_address"), 200),
                    facility_city,
                    lat, lng,
                    text(body.get("facility_phone"), 40),
                    now_iso(),
                ),
            )

        user = {
            "id": new_id("usr"),
            "name": text(body["name"], 100),
            "email": email,
            "phone": text(body["phone"], 40),
            "password_hash": generate_password_hash(text(body["password"], 200), method="pbkdf2:sha256"),
            "role": role,
            "city": text(body.get("city"), 100),
            "lat": body.get("lat") if role != "provider" else None,
            "lng": body.get("lng") if role != "provider" else None,
            "facility_id": facility_id,
            "created_at": now_iso(),
        }
        conn.execute(
            "INSERT INTO users (id,name,email,phone,password_hash,role,city,lat,lng,facility_id,created_at) "
            "VALUES (:id,:name,:email,:phone,:password_hash,:role,:city,:lat,:lng,:facility_id,:created_at)",
            user,
        )
        if role == "patient":
            conn.execute(
                "INSERT INTO medical_profiles (user_id,updated_at) VALUES (?,?)",
                (user["id"], now_iso()),
            )

    session.permanent = True
    session["uid"] = user["id"]
    write_snapshot()
    return ok(user=public_user(user), message="Account created.")


@app.route("/api/auth/login", methods=["POST"])
def login():
    body = request.get_json(silent=True) or {}
    error = fields(body, ["email", "password"])
    if error:
        return fail(error)
    email = text(body["email"], 150).lower()
    with db() as conn:
        row = conn.execute("SELECT * FROM users WHERE email=?", (email,)).fetchone()
    if not row or not check_password_hash(row["password_hash"], str(body["password"])):
        return fail("Incorrect email or password.", 401)
    user = dict(row)
    session.permanent = True
    session["uid"] = user["id"]
    return ok(user=public_user(user))


@app.route("/api/auth/logout", methods=["POST"])
def logout():
    session.clear()
    return ok()


def public_user(user: dict) -> dict:
    return {
        "id": user["id"],
        "name": user["name"],
        "email": user["email"],
        "phone": user["phone"],
        "role": user["role"],
        "city": user["city"],
    }


@app.route("/api/auth/me")
@login_required
def me():
    return ok(user=public_user(g.user))


# ---------------------------------------------------------------------------
# Account profile (all roles) and notification inbox
# ---------------------------------------------------------------------------


@app.route("/api/me/profile")
@login_required
def my_profile():
    with db() as conn:
        summary = {}
        if g.user["role"] == "patient":
            summary["caregivers"] = conn.execute(
                "SELECT COUNT(*) FROM relationships WHERE patient_id=? AND status='active'",
                (g.user["id"],),
            ).fetchone()[0]
            summary["medicines"] = conn.execute(
                "SELECT COUNT(*) FROM medications WHERE patient_id=? AND active=1",
                (g.user["id"],),
            ).fetchone()[0]
        elif g.user["role"] == "caregiver":
            summary["patients"] = conn.execute(
                "SELECT COUNT(*) FROM relationships WHERE caregiver_id=? AND status='active'",
                (g.user["id"],),
            ).fetchone()[0]
        elif g.user["role"] == "provider" and g.user.get("facility_id"):
            facility = conn.execute(
                "SELECT name,city FROM facilities WHERE id=?", (g.user["facility_id"],)
            ).fetchone()
            summary["facility"] = facility["name"] if facility else "Unknown facility"
            if facility and facility["city"]:
                summary["facility"] += f" · {facility['city']}"
    return ok(user=public_user(g.user), summary=summary)


@app.route("/api/me/profile", methods=["PUT"])
@login_required
def update_my_profile():
    body = request.get_json(silent=True) or {}
    error = fields(body, ["name", "phone"])
    if error:
        return fail(error)
    with db() as conn:
        conn.execute(
            "UPDATE users SET name=?,phone=?,city=? WHERE id=?",
            (text(body["name"], 100), text(body["phone"], 40), text(body.get("city"), 100), g.user["id"]),
        )
        add_log(conn, g.user["id"] if g.user["role"] == "patient" else None,
                g.user["id"], "account_updated", "Account profile updated.")
        user = row_dict(conn.execute("SELECT * FROM users WHERE id=?", (g.user["id"],)).fetchone())
    return ok(user=public_user(user), message="Profile saved.")


@app.route("/api/notifications")
@login_required
def list_notifications():
    with db() as conn:
        rows = conn.execute(
            "SELECT * FROM notifications WHERE user_id=? ORDER BY created_at DESC LIMIT 40",
            (g.user["id"],),
        ).fetchall()
        unread = conn.execute(
            "SELECT COUNT(*) FROM notifications WHERE user_id=? AND read=0", (g.user["id"],)
        ).fetchone()[0]
    items = []
    for row in rows:
        item = row_dict(row)
        try:
            item["data"] = json.loads(item.get("data") or "{}")
        except (TypeError, ValueError):
            item["data"] = {}
        items.append(item)
    return ok(notifications=items, unread=unread)


@app.route("/api/notifications/<nid>/read", methods=["POST"])
@login_required
def read_notification(nid):
    with db() as conn:
        row = conn.execute("SELECT * FROM notifications WHERE id=? AND user_id=?", (nid, g.user["id"])).fetchone()
        if not row:
            return fail("Notification not found.", 404)
        conn.execute("UPDATE notifications SET read=1 WHERE id=?", (nid,))
        unread = conn.execute(
            "SELECT COUNT(*) FROM notifications WHERE user_id=? AND read=0", (g.user["id"],)
        ).fetchone()[0]
    return ok(unread=unread)


@app.route("/api/notifications/read-all", methods=["POST"])
@login_required
def read_all_notifications():
    with db() as conn:
        conn.execute("UPDATE notifications SET read=1 WHERE user_id=? AND read=0", (g.user["id"],))
    return ok(unread=0)


# ---------------------------------------------------------------------------
# Family consent / relationships
# ---------------------------------------------------------------------------


@app.route("/api/family/invite", methods=["POST"])
@role_required("patient")
def create_invite():
    with db() as conn:
        code = secrets.token_urlsafe(6).upper()
        conn.execute(
            "INSERT INTO invite_codes (code,patient_id,created_at,expires_at,used) VALUES (?,?,?,?,0)",
            (code, g.user["id"], now_iso(), (datetime.now() + timedelta(hours=24)).isoformat(timespec="seconds")),
        )
    return ok(code=code, expires_hours=24)


@app.route("/api/family/request", methods=["POST"])
@role_required("caregiver")
def request_relationship():
    body = request.get_json(silent=True) or {}
    code = text(body.get("code"), 30).upper()
    if not code:
        return fail("Enter the invite code shared by the patient.")
    with db() as conn:
        invite = conn.execute("SELECT * FROM invite_codes WHERE code=?", (code,)).fetchone()
        if not invite or invite["used"] or invite["expires_at"] < now_iso():
            return fail("That invite code is invalid or expired.", 400)
        patient = safe_patient(conn, invite["patient_id"])
        if not patient:
            return fail("Invite owner no longer exists.", 400)
        conn.execute(
            "INSERT OR IGNORE INTO relationships "
            "(id,patient_id,caregiver_id,status,can_manage_meds,can_view_health,created_at,updated_at) "
            "VALUES (?,?,?,'pending',1,0,?,?)",
            (new_id("rel"), patient["id"], g.user["id"], now_iso(), now_iso()),
        )
        add_log(conn, patient["id"], g.user["id"], "family_request", f"{g.user['name']} requested access.")
    return ok(message="Request sent. Wait for the patient to approve it.")


@app.route("/api/family/requests")
@role_required("patient")
def list_requests():
    with db() as conn:
        rows = conn.execute(
            "SELECT r.*, u.name AS caregiver_name, u.email AS caregiver_email "
            "FROM relationships r JOIN users u ON u.id=r.caregiver_id "
            "WHERE r.patient_id=? ORDER BY r.created_at DESC",
            (g.user["id"],),
        ).fetchall()
    return ok(relationships=[row_dict(r) for r in rows])


@app.route("/api/family/<rel_id>/approve", methods=["POST"])
@role_required("patient")
def approve_relationship(rel_id):
    body = request.get_json(silent=True) or {}
    with db() as conn:
        rel = conn.execute(
            "SELECT * FROM relationships WHERE id=? AND patient_id=?", (rel_id, g.user["id"])
        ).fetchone()
        if not rel:
            return fail("Request not found.", 404)
        conn.execute(
            "UPDATE relationships SET status='active', can_manage_meds=?, can_view_health=?, updated_at=? WHERE id=?",
            (1 if body.get("can_manage_meds", True) else 0, 1 if body.get("can_view_health", False) else 0, now_iso(), rel_id),
        )
        conn.execute("UPDATE invite_codes SET used=1 WHERE patient_id=?", (g.user["id"],))
        add_log(conn, g.user["id"], g.user["id"], "family_approved", "Caregiver access approved.")
    return ok(message="Caregiver approved.")


@app.route("/api/family/<rel_id>/decline", methods=["POST"])
@role_required("patient")
def decline_relationship(rel_id):
    with db() as conn:
        conn.execute(
            "UPDATE relationships SET status='declined', updated_at=? WHERE id=? AND patient_id=?",
            (now_iso(), rel_id, g.user["id"]),
        )
    return ok()


@app.route("/api/family/patients")
@role_required("caregiver")
def caregiver_patients():
    with db() as conn:
        rows = conn.execute(
            "SELECT u.id,u.name,u.city,r.can_manage_meds,r.can_view_health "
            "FROM relationships r JOIN users u ON u.id=r.patient_id "
            "WHERE r.caregiver_id=? AND r.status='active' ORDER BY u.name",
            (g.user["id"],),
        ).fetchall()
    return ok(patients=[row_dict(r) for r in rows])


# ---------------------------------------------------------------------------
# Medical profile, emergency contacts, devices
# ---------------------------------------------------------------------------


@app.route("/api/patients/<pid>/profile")
@login_required
def get_profile(pid):
    with db() as conn:
        denied = access_or_403(conn, pid, health=True)
        if denied:
            return denied
        profile = row_dict(conn.execute("SELECT * FROM medical_profiles WHERE user_id=?", (pid,)).fetchone())
        patient = safe_patient(conn, pid)
    if not profile:
        return fail("Profile not found.", 404)
    return ok(profile=profile, patient=patient)


@app.route("/api/patients/<pid>/profile", methods=["PUT"])
@login_required
def save_profile(pid):
    body = request.get_json(silent=True) or {}
    with db() as conn:
        denied = access_or_403(conn, pid)
        if denied:
            return denied
        conn.execute(
            "UPDATE medical_profiles SET blood_group=?,allergies=?,conditions=?,doctor_name=?,"
            "doctor_phone=?,share_medical=?,updated_at=? WHERE user_id=?",
            (
                text(body.get("blood_group"), 20),
                text(body.get("allergies"), 300),
                text(body.get("conditions"), 300),
                text(body.get("doctor_name"), 120),
                text(body.get("doctor_phone"), 40),
                1 if body.get("share_medical") else 0,
                now_iso(),
                pid,
            ),
        )
        add_log(conn, pid, g.user["id"], "profile_updated", "Medical profile updated.")
    return ok(message="Profile saved.")


@app.route("/api/patients/<pid>/emergency-contacts")
@login_required
def list_contacts(pid):
    with db() as conn:
        denied = access_or_403(conn, pid, health=True)
        if denied:
            return denied
        rows = conn.execute("SELECT * FROM emergency_contacts WHERE patient_id=? ORDER BY created_at", (pid,)).fetchall()
    return ok(contacts=[row_dict(r) for r in rows])


@app.route("/api/patients/<pid>/emergency-contacts", methods=["POST"])
@login_required
def add_contact(pid):
    body = request.get_json(silent=True) or {}
    error = fields(body, ["name", "phone"])
    if error:
        return fail(error)
    with db() as conn:
        denied = access_or_403(conn, pid)
        if denied:
            return denied
        item = {
            "id": new_id("contact"),
            "patient_id": pid,
            "name": text(body["name"], 100),
            "phone": text(body["phone"], 40),
            "relation": text(body.get("relation"), 60),
            "verified": 0,
            "created_at": now_iso(),
        }
        conn.execute(
            "INSERT INTO emergency_contacts (id,patient_id,name,phone,relation,verified,created_at) "
            "VALUES (:id,:patient_id,:name,:phone,:relation,:verified,:created_at)",
            item,
        )
        add_log(conn, pid, g.user["id"], "contact_added", f"Emergency contact {item['name']} added (unverified).")
    return ok(contact=item, warning="Contact is unverified until you confirm their phone.")


@app.route("/api/emergency-contacts/<cid>/verify", methods=["POST"])
@login_required
def verify_contact(cid):
    with db() as conn:
        row = conn.execute("SELECT * FROM emergency_contacts WHERE id=?", (cid,)).fetchone()
        if not row:
            return fail("Contact not found.", 404)
        denied = access_or_403(conn, row["patient_id"])
        if denied:
            return denied
        conn.execute("UPDATE emergency_contacts SET verified=1 WHERE id=?", (cid,))
    return ok(message="Contact marked verified.")


@app.route("/api/emergency-contacts/<cid>", methods=["DELETE"])
@login_required
def delete_contact(cid):
    with db() as conn:
        row = conn.execute("SELECT * FROM emergency_contacts WHERE id=?", (cid,)).fetchone()
        if not row:
            return fail("Contact not found.", 404)
        denied = access_or_403(conn, row["patient_id"])
        if denied:
            return denied
        conn.execute("DELETE FROM emergency_contacts WHERE id=?", (cid,))
    return ok()


@app.route("/api/devices", methods=["POST"])
@login_required
def register_device():
    body = request.get_json(silent=True) or {}
    token = text(body.get("token"), 300)
    if not token:
        return fail("Device token is required.")
    with db() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO devices (id,user_id,token,platform,created_at) VALUES (?,?,?,?,?)",
            (new_id("dev"), g.user["id"], token, text(body.get("platform"), 40) or "browser", now_iso()),
        )
    return ok(message="Device registered for future push delivery.")


# ---------------------------------------------------------------------------
# Medication schedules and adherence
# ---------------------------------------------------------------------------


def today_event(conn, med_id: str):
    today = datetime.now().strftime("%Y-%m-%d")
    return conn.execute(
        "SELECT * FROM medication_events WHERE medication_id=? AND substr(due_at,1,10)=? "
        "ORDER BY created_at DESC LIMIT 1",
        (med_id, today),
    ).fetchone()


@app.route("/api/patients/<pid>/medications")
@login_required
def list_medications(pid):
    with db() as conn:
        denied = access_or_403(conn, pid)
        if denied:
            return denied
        rows = conn.execute(
            "SELECT * FROM medications WHERE patient_id=? AND active=1 ORDER BY schedule_time", (pid,)
        ).fetchall()
        result = []
        for row in rows:
            item = row_dict(row)
            event = today_event(conn, item["id"])
            item["today_status"] = event["status"] if event else "scheduled"
            item["today_due_at"] = event["due_at"] if event else None
            result.append(item)
    return ok(medications=result)


@app.route("/api/patients/<pid>/medications", methods=["POST"])
@login_required
def add_medication(pid):
    body = request.get_json(silent=True) or {}
    error = fields(body, ["name", "schedule_time"])
    if error:
        return fail(error)
    try:
        datetime.strptime(text(body["schedule_time"], 5), "%H:%M")
    except ValueError:
        return fail("Schedule time must use HH:MM (24-hour), e.g. 20:30.")
    with db() as conn:
        denied = access_or_403(conn, pid, manage=True)
        if denied:
            return denied
        med = {
            "id": new_id("med"),
            "patient_id": pid,
            "name": text(body["name"], 120),
            "dose": text(body.get("dose"), 60),
            "condition": text(body.get("condition"), 120),
            "schedule_time": text(body["schedule_time"], 5),
            "instructions": text(body.get("instructions"), 300),
            "created_by": g.user["id"],
            "active": 1,
            "created_at": now_iso(),
        }
        conn.execute(
            "INSERT INTO medications (id,patient_id,name,dose,condition,schedule_time,instructions,created_by,active,created_at) "
            "VALUES (:id,:patient_id,:name,:dose,:condition,:schedule_time,:instructions,:created_by,:active,:created_at)",
            med,
        )
        add_log(conn, pid, g.user["id"], "med_added", f"Medication '{med['name']}' scheduled for {med['schedule_time']}.")
        notification = create_notification(
            conn,
            pid,
            "medication_added",
            "New medicine scheduled",
            f"{g.user['name']} added {med['name']}{(' ' + med['dose']) if med['dose'] else ''} at {med['schedule_time']}.",
            {"medication_id": med["id"], "schedule_time": med["schedule_time"]},
        )
    event = {
        "patient_id": pid,
        "medication_id": med["id"],
        "name": med["name"],
        "schedule_time": med["schedule_time"],
        "dose": med["dose"],
        "added_by": g.user["name"],
        "notification_id": notification["id"],
    }
    for uid in stakeholder_ids_conn(pid):
        emit(uid, "MEDICATIONS_UPDATED", event)
    return ok(medication=med, notification=notification)


def stakeholder_ids_conn(pid: str) -> list[str]:
    with db() as conn:
        return stakeholder_ids(conn, pid)


@app.route("/api/medications/<mid>", methods=["DELETE"])
@login_required
def delete_medication(mid):
    with db() as conn:
        row = conn.execute("SELECT * FROM medications WHERE id=?", (mid,)).fetchone()
        if not row:
            return fail("Medication not found.", 404)
        denied = access_or_403(conn, row["patient_id"], manage=True)
        if denied:
            return denied
        conn.execute("UPDATE medications SET active=0 WHERE id=?", (mid,))
        add_log(conn, row["patient_id"], g.user["id"], "med_removed", f"Medication '{row['name']}' removed.")
        pid = row["patient_id"]
    for uid in stakeholder_ids_conn(pid):
        emit(uid, "MEDICATIONS_UPDATED", {"patient_id": pid})
    return ok()


@app.route("/api/medications/<mid>/acknowledge", methods=["POST"])
@login_required
def acknowledge_medication(mid):
    body = request.get_json(silent=True) or {}
    action = body.get("action", "taken")
    if action not in ("taken", "snooze"):
        return fail("Action must be 'taken' or 'snooze'.")
    with db() as conn:
        med = conn.execute("SELECT * FROM medications WHERE id=?", (mid,)).fetchone()
        if not med:
            return fail("Medication not found.", 404)
        pid = med["patient_id"]
        denied = access_or_403(conn, pid)
        if denied:
            return denied
        event = today_event(conn, mid)
        if not event:
            conn.execute(
                "INSERT INTO medication_events (id,medication_id,patient_id,due_at,status,acknowledged_at,acknowledged_by,created_at) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (new_id("mev"), mid, pid, now_iso(), "taken", now_iso(), g.user["id"], now_iso()),
            )
        else:
            conn.execute(
                "UPDATE medication_events SET status=?,acknowledged_at=?,acknowledged_by=? WHERE id=?",
                (action, now_iso(), g.user["id"], event["id"]),
            )
        if action == "snooze":
            conn.execute(
                "INSERT INTO medication_events (id,medication_id,patient_id,due_at,status,acknowledged_at,acknowledged_by,created_at) "
                "VALUES (?,?,?,?, 'pending',NULL,NULL,?)",
                (new_id("mev"), mid, pid, (datetime.now() + timedelta(minutes=10)).isoformat(timespec="seconds"), now_iso()),
            )
        add_log(conn, pid, g.user["id"], "med_taken" if action == "taken" else "med_snoozed",
                f"{g.user['name']} marked '{med['name']}' as {action}.")
        result = {
            "medication_id": mid,
            "status": action,
            "by": g.user["name"],
            "at": now_iso(),
        }
    for uid in stakeholder_ids_conn(pid):
        emit(uid, "MEDICATION_ACKNOWLEDGED", result)
    return ok(result=result)


# ---------------------------------------------------------------------------
# Instant family alerts
# ---------------------------------------------------------------------------

ALERT_TYPES = {
    "medicine": "Take Your Medicine",
    "check_phone": "Please Check Your Phone",
    "health_checkin": "Health Check-In",
    "emergency_call": "Emergency: Contact Me Immediately",
    "custom": "Custom Alert",
}


@app.route("/api/patients/<pid>/alerts")
@login_required
def list_alerts(pid):
    with db() as conn:
        denied = access_or_403(conn, pid)
        if denied:
            return denied
        rows = conn.execute(
            "SELECT * FROM alerts WHERE patient_id=? ORDER BY created_at DESC LIMIT 50", (pid,)
        ).fetchall()
        deliveries = conn.execute(
            "SELECT * FROM deliveries WHERE event_type='alert' AND event_id IN "
            "(SELECT id FROM alerts WHERE patient_id=?)",
            (pid,),
        ).fetchall()
    by_event: dict[str, list] = {}
    for d in deliveries:
        by_event.setdefault(d["event_id"], []).append(row_dict(d))
    alerts = []
    for row in rows:
        item = row_dict(row)
        item["deliveries"] = by_event.get(item["id"], [])
        alerts.append(item)
    return ok(alerts=alerts, types=ALERT_TYPES)


@app.route("/api/patients/<pid>/alerts", methods=["POST"])
@login_required
def send_alert(pid):
    body = request.get_json(silent=True) or {}
    alert_type = body.get("type", "custom")
    if alert_type not in ALERT_TYPES:
        return fail("Unknown alert type.")
    message = text(body.get("message"), 400) or ALERT_TYPES[alert_type]
    with db() as conn:
        denied = access_or_403(conn, pid)
        if denied:
            return denied
        alert = {
            "id": new_id("alt"),
            "patient_id": pid,
            "sender_id": g.user["id"],
            "alert_type": alert_type,
            "message": message,
            "status": "sent",
            "created_at": now_iso(),
        }
        conn.execute(
            "INSERT INTO alerts (id,patient_id,sender_id,alert_type,message,status,created_at) "
            "VALUES (:id,:patient_id,:sender_id,:alert_type,:message,:status,:created_at)",
            alert,
        )
        recipients = stakeholder_ids(conn, pid)
        for uid in recipients:
            create_delivery(conn, "alert", alert["id"], "in_app", uid, f"In-app alert to {uid[:8]}", "sent")
        devices = conn.execute("SELECT * FROM devices WHERE user_id IN (%s)" % ",".join("?" * len(recipients)), recipients).fetchall()
        for device in devices:
            create_delivery(
                conn, "alert", alert["id"], "web_push", device["user_id"], device["platform"],
                "pending",
            )
        add_log(conn, pid, g.user["id"], "alert_sent", f"{g.user['name']} sent: {message}")
        note = create_notification(
            conn, pid, "alert", ALERT_TYPES[alert_type], message,
            {"alert_id": alert["id"], "sender": g.user["name"]},
        )
        alert["sender_name"] = g.user["name"]
        alert["type_label"] = ALERT_TYPES[alert_type]
        alert["notification_id"] = note["id"]
    for uid in recipients:
        emit(uid, "ALERT_CREATED", alert)
    write_snapshot()
    return ok(alert=alert, recipients=len(recipients))


@app.route("/api/alerts/<aid>/acknowledge", methods=["POST"])
@login_required
def acknowledge_alert(aid):
    with db() as conn:
        alert = conn.execute("SELECT * FROM alerts WHERE id=?", (aid,)).fetchone()
        if not alert:
            return fail("Alert not found.", 404)
        denied = access_or_403(conn, alert["patient_id"])
        if denied:
            return denied
        conn.execute(
            "UPDATE alerts SET status='acknowledged',acknowledged_at=?,acknowledged_by=? WHERE id=?",
            (now_iso(), g.user["id"], aid),
        )
        conn.execute(
            "UPDATE deliveries SET status='acknowledged',updated_at=? WHERE event_type='alert' AND event_id=? AND recipient_id=?",
            (now_iso(), aid, g.user["id"]),
        )
        add_log(conn, alert["patient_id"], g.user["id"], "alert_ack", "Alert acknowledged.")
        pid = alert["patient_id"]
        result = {"alert_id": aid, "by": g.user["name"], "at": now_iso()}
    for uid in stakeholder_ids_conn(pid):
        emit(uid, "ALERT_ACKNOWLEDGED", result)
    return ok(result=result)


# ---------------------------------------------------------------------------
# Emergency SOS - request only, never dispatch claims
# ---------------------------------------------------------------------------


@app.route("/api/patients/<pid>/sos")
@login_required
def list_sos(pid):
    with db() as conn:
        denied = access_or_403(conn, pid, health=True)
        if denied:
            return denied
        rows = conn.execute(
            "SELECT * FROM sos_events WHERE patient_id=? ORDER BY created_at DESC LIMIT 25", (pid,)
        ).fetchall()
        result = []
        for row in rows:
            item = row_dict(row)
            item["deliveries"] = [
                row_dict(d) for d in conn.execute(
                    "SELECT * FROM deliveries WHERE event_type='sos' AND event_id=?", (item["id"],)
                )
            ]
            result.append(item)
    return ok(sos_events=result)


@app.route("/api/patients/<pid>/sos", methods=["POST"])
@login_required
def trigger_sos(pid):
    body = request.get_json(silent=True) or {}
    notes = text(body.get("notes"), 400) or "Emergency help requested."
    with db() as conn:
        denied = access_or_403(conn, pid)
        if denied:
            return denied
        patient = safe_patient(conn, pid)
        if not patient:
            return fail("Patient not found.", 404)

        # Accidental-activation guard: 60 second backend cooldown.
        recent = conn.execute(
            "SELECT created_at FROM sos_events WHERE patient_id=? ORDER BY created_at DESC LIMIT 1", (pid,)
        ).fetchone()
        if recent:
            try:
                elapsed = (datetime.now() - datetime.fromisoformat(recent["created_at"])).total_seconds()
                if elapsed < 60:
                    return fail("An SOS was just sent. Please wait a moment before sending another.", 429)
            except ValueError:
                pass

        lat, lng = body.get("lat"), body.get("lng")
        source = "browser_geolocation"
        if lat in (None, "") or lng in (None, ""):
            lat, lng = patient["lat"], patient["lng"]
            source = "saved_profile_location"
        try:
            lat = float(lat) if lat not in (None, "") else None
            lng = float(lng) if lng not in (None, "") else None
        except (TypeError, ValueError):
            return fail("Invalid location coordinates.")
        if lat is None or lng is None:
            # The alert still goes out; only location is unavailable.
            source = "unavailable"

        sos = {
            "id": new_id("sos"),
            "patient_id": pid,
            "created_by": g.user["id"],
            "lat": lat,
            "lng": lng,
            "accuracy": body.get("accuracy"),
            "location_source": source,
            "notes": notes,
            "status": "open",
            "created_at": now_iso(),
        }
        conn.execute(
            "INSERT INTO sos_events (id,patient_id,created_by,lat,lng,accuracy,location_source,notes,status,created_at) "
            "VALUES (:id,:patient_id,:created_by,:lat,:lng,:accuracy,:location_source,:notes,:status,:created_at)",
            sos,
        )

        recipients = stakeholder_ids(conn, pid)
        for uid in recipients:
            create_delivery(conn, "sos", sos["id"], "in_app", uid, f"Urgent SOS to {uid[:8]}", "sent")
            create_notification(
                conn, uid, "sos", "Emergency request sent",
                notes,
                {"sos_id": sos["id"], "location_source": source},
            )

        contacts = conn.execute(
            "SELECT * FROM emergency_contacts WHERE patient_id=?", (pid,)
        ).fetchall()
        for contact in contacts:
            label = contact["name"] + (" (verified)" if contact["verified"] else " (unverified)")
            create_delivery(
                conn, "sos", sos["id"], "contact_sms", contact["id"], label,
                "pending" if contact["verified"] else "failed",
            )
            if not contact["verified"]:
                conn.execute(
                    "UPDATE deliveries SET last_error=? WHERE event_type='sos' AND event_id=? AND recipient_id=?",
                    ("SMS not configured / contact unverified", sos["id"], contact["id"]),
                )

        facilities = conn.execute(
            "SELECT * FROM facilities WHERE verified=1 ORDER BY name"
        ).fetchall()
        routed_facilities = []
        facility_user_ids = set()
        for facility in facilities:
            reasons = []
            if facility["city"] and patient["city"]:
                if facility["city"].strip().lower() == patient["city"].strip().lower():
                    reasons.append("same city")
            if None not in (facility["lat"], facility["lng"], lat, lng):
                distance = haversine(lat, lng, facility["lat"], facility["lng"])
                if distance <= 25:
                    reasons.append(f"~{distance:.1f} km")
            # Registered desks always receive the request record so the provider
            # dashboard has actionable data. Matching only changes the label.
            reasons.append("registered request desk")
            create_delivery(
                conn,
                "sos",
                sos["id"],
                "facility_integration",
                facility["id"],
                f"{facility['name']} ({'; '.join(reasons)})",
                "sent",
            )
            routed_facilities.append(facility["id"])
            for provider in conn.execute(
                "SELECT id FROM users WHERE role='provider' AND facility_id=?", (facility["id"],)
            ):
                facility_user_ids.add(provider["id"])

        add_log(
            conn, pid, g.user["id"], "sos_opened",
            f"{g.user['name']} requested emergency help. Location source: {source}.",
        )

        profile = row_dict(conn.execute("SELECT * FROM medical_profiles WHERE user_id=?", (pid,)).fetchone()) or {}
        sos["patient_id"] = pid
        sos["patient_name"] = patient["name"]
        sos["patient_phone"] = patient["phone"]
        sos["patient_city"] = patient["city"]
        sos["medical"] = profile if profile.get("share_medical") else {"share_medical": False}
        sos["deliveries"] = [
            row_dict(d) for d in conn.execute(
                "SELECT * FROM deliveries WHERE event_type='sos' AND event_id=?", (sos["id"],)
            )
        ]

    for uid in set(recipients) | facility_user_ids:
        emit(uid, "SOS_CREATED", sos)
    write_snapshot()
    return ok(sos=sos, warning="This is a help request only. No ambulance is dispatched by this app.")


@app.route("/api/sos/<sid>")
@login_required
def get_sos(sid):
    with db() as conn:
        sos = conn.execute("SELECT * FROM sos_events WHERE id=?", (sid,)).fetchone()
        if not sos:
            return fail("SOS not found.", 404)
        pid = sos["patient_id"]
        if g.user["role"] == "provider":
            allowed = conn.execute(
                "SELECT 1 FROM deliveries WHERE event_type='sos' AND event_id=? AND recipient_id=?",
                (sid, g.user["facility_id"]),
            ).fetchone()
        else:
            allowed = can_access(conn, g.user, pid, health=True)
        if not allowed:
            return fail("You are not authorized for this emergency.", 403)
        result = row_dict(sos)
        result["deliveries"] = [
            row_dict(d) for d in conn.execute(
                "SELECT * FROM deliveries WHERE event_type='sos' AND event_id=?", (sid,)
            )
        ]
        patient = safe_patient(conn, pid) or {}
        result["patient_id"] = pid
        result["patient_name"] = patient.get("name") or "Unknown"
        result["patient_phone"] = patient.get("phone") or ""
        result["patient_city"] = patient.get("city") or ""
        profile = row_dict(conn.execute("SELECT * FROM medical_profiles WHERE user_id=?", (pid,)).fetchone()) or {}
        result["medical"] = profile if profile.get("share_medical") else {"share_medical": False}
    return ok(sos=result)


@app.route("/api/sos/<sid>/acknowledge", methods=["POST"])
@login_required
def acknowledge_sos(sid):
    with db() as conn:
        sos = conn.execute("SELECT * FROM sos_events WHERE id=?", (sid,)).fetchone()
        if not sos:
            return fail("SOS not found.", 404)
        pid = sos["patient_id"]
        if g.user["role"] == "provider":
            delivery = conn.execute(
                "SELECT * FROM deliveries WHERE event_type='sos' AND event_id=? AND recipient_id=?",
                (sid, g.user["facility_id"]),
            ).fetchone()
        else:
            delivery = conn.execute(
                "SELECT * FROM deliveries WHERE event_type='sos' AND event_id=? AND recipient_id=?",
                (sid, g.user["id"]),
            ).fetchone()
        if not delivery:
            return fail("You were not a recipient of this emergency.", 403)
        conn.execute(
            "UPDATE deliveries SET status='acknowledged',updated_at=? WHERE id=?", (now_iso(), delivery["id"])
        )
        conn.execute(
            "UPDATE sos_events SET status='acknowledged' WHERE id=? AND status='open'", (sid,)
        )
        add_log(conn, pid, g.user["id"], "sos_ack", f"{g.user['name']} acknowledged the emergency request.")
        result = {"sos_id": sid, "by": g.user["name"], "at": now_iso()}
        recipients = stakeholder_ids(conn, pid)
    for uid in recipients:
        emit(uid, "SOS_UPDATED", result)
    return ok(result=result)


@app.route("/api/sos/<sid>/respond", methods=["POST"])
@role_required("provider")
def provider_respond(sid):
    body = request.get_json(silent=True) or {}
    status = body.get("status")
    if status not in ("acknowledged", "unable"):
        return fail("Status must be 'acknowledged' or 'unable'.")
    note = text(body.get("note"), 300)
    with db() as conn:
        sos = conn.execute("SELECT * FROM sos_events WHERE id=?", (sid,)).fetchone()
        if not sos:
            return fail("SOS not found.", 404)
        delivery = conn.execute(
            "SELECT * FROM deliveries WHERE event_type='sos' AND event_id=? AND recipient_id=?",
            (sid, g.user["facility_id"]),
        ).fetchone()
        if not delivery:
            return fail("Your facility was not contacted for this emergency.", 403)
        new_status = "acknowledged" if status == "acknowledged" else "failed"
        conn.execute(
            "UPDATE deliveries SET status=?,last_error=?,updated_at=? WHERE id=?",
            (new_status, note or None, now_iso(), delivery["id"]),
        )
        if status == "acknowledged":
            conn.execute("UPDATE sos_events SET status='acknowledged' WHERE id=? AND status='open'", (sid,))
        add_log(
            conn, sos["patient_id"], g.user["id"], "facility_response",
            f"Facility response: {status}. {note}".strip(),
        )
        result = {"sos_id": sid, "status": status, "facility": g.user["name"], "note": note}
        recipients = stakeholder_ids(conn, sos["patient_id"])
    for uid in recipients:
        emit(uid, "SOS_UPDATED", result)
    return ok(result=result, warning="No ambulance dispatch is implied by this response.")


@app.route("/api/sos/<sid>/resolve", methods=["POST"])
@login_required
def resolve_sos(sid):
    with db() as conn:
        sos = conn.execute("SELECT * FROM sos_events WHERE id=?", (sid,)).fetchone()
        if not sos:
            return fail("SOS not found.", 404)
        if g.user["role"] == "provider":
            return fail("Only family or the patient may close an emergency.", 403)
        denied = access_or_403(conn, sos["patient_id"])
        if denied:
            return denied
        conn.execute("UPDATE sos_events SET status='resolved',resolved_at=? WHERE id=?", (now_iso(), sid))
        add_log(conn, sos["patient_id"], g.user["id"], "sos_resolved", "Emergency marked as resolved.")
        result = {"sos_id": sid, "status": "resolved", "by": g.user["name"]}
        recipients = stakeholder_ids(conn, sos["patient_id"])
    for uid in recipients:
        emit(uid, "SOS_UPDATED", result)
    return ok(result=result)


@app.route("/api/provider/requests")
@role_required("provider")
def provider_requests():
    with db() as conn:
        facility = row_dict(conn.execute(
            "SELECT id,name,address,city,phone,verified,integration_status FROM facilities WHERE id=?",
            (g.user["facility_id"],),
        ).fetchone())
        ensure_facility_deliveries(conn, g.user["facility_id"])
        rows = conn.execute(
            "SELECT DISTINCT s.* FROM sos_events s JOIN deliveries d ON d.event_id=s.id "
            "WHERE d.event_type='sos' AND d.recipient_id=? ORDER BY s.created_at DESC",
            (g.user["facility_id"],),
        ).fetchall()
        result = []
        for row in rows:
            item = row_dict(row)
            item["deliveries"] = [
                row_dict(d) for d in conn.execute(
                    "SELECT * FROM deliveries WHERE event_type='sos' AND event_id=?", (item["id"],)
                )
            ]
            patient = safe_patient(conn, item["patient_id"]) or {}
            item["patient_name"] = patient.get("name") or "Unknown"
            item["patient_phone"] = patient.get("phone") or ""
            item["patient_city"] = patient.get("city") or ""
            profile = row_dict(conn.execute(
                "SELECT * FROM medical_profiles WHERE user_id=?", (item["patient_id"],)
            ).fetchone()) or {}
            item["medical"] = profile if profile.get("share_medical") else {"share_medical": False}
            result.append(item)
        counts = {
            "open": sum(1 for item in result if item["status"] == "open"),
            "acknowledged": sum(1 for item in result if item["status"] == "acknowledged"),
            "total": len(result),
        }
    return ok(requests=result, facility=facility, counts=counts)


# ---------------------------------------------------------------------------
# Activity feed
# ---------------------------------------------------------------------------


@app.route("/api/patients/<pid>/activity")
@login_required
def activity(pid):
    with db() as conn:
        denied = access_or_403(conn, pid, health=True)
        if denied:
            return denied
        rows = conn.execute(
            "SELECT * FROM audit_logs WHERE patient_id=? ORDER BY created_at DESC LIMIT 50", (pid,)
        ).fetchall()
    return ok(activity=[row_dict(r) for r in rows])


# ---------------------------------------------------------------------------
# Server-Sent Events: authenticated, per-user stream
# ---------------------------------------------------------------------------


@app.route("/api/events")
@login_required
def events():
    user_id = g.user["id"]
    q = queue.Queue(maxsize=100)
    with queues_lock:
        user_queues.setdefault(user_id, []).append(q)

    def stream():
        try:
            hello = json.dumps({"type": "CONNECTED", "data": {"user_id": user_id}, "timestamp": now_iso()})
            yield f"data: {hello}\n\n"
            while True:
                try:
                    yield f"data: {q.get(timeout=20)}\n\n"
                except queue.Empty:
                    yield ": keepalive\n\n"
        finally:
            with queues_lock:
                if q in user_queues.get(user_id, []):
                    user_queues[user_id].remove(q)

    return Response(
        stream(),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ---------------------------------------------------------------------------
# Background workers: due doses, retry, escalation
# ---------------------------------------------------------------------------


def scheduler_worker():
    while True:
        try:
            now = datetime.now()
            today = now.strftime("%Y-%m-%d")
            notifications = []
            with db() as conn:
                meds = conn.execute("SELECT * FROM medications WHERE active=1").fetchall()
                for med in meds:
                    hh, mm = [int(x) for x in med["schedule_time"].split(":")]
                    due = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
                    if now < due:
                        continue
                    existing = conn.execute(
                        "SELECT 1 FROM medication_events WHERE medication_id=? AND substr(due_at,1,10)=? AND status IN ('pending','taken','snoozed')",
                        (med["id"], today),
                    ).fetchone()
                    if existing:
                        continue
                    conn.execute(
                        "INSERT INTO medication_events (id,medication_id,patient_id,due_at,status,created_at) "
                        "VALUES (?,?,?,?,'pending',?)",
                        (new_id("mev"), med["id"], med["patient_id"], due.isoformat(timespec="seconds"), now_iso()),
                    )
                    payload = {
                        "medication_id": med["id"],
                        "name": med["name"],
                        "dose": med["dose"],
                        "due_at": due.isoformat(timespec="seconds"),
                        "patient_id": med["patient_id"],
                    }
                    recipients = stakeholder_ids(conn, med["patient_id"])
                    notification_ids = []
                    for uid in recipients:
                        is_patient = uid == med["patient_id"]
                        note = create_notification(
                            conn,
                            uid,
                            "medication_due",
                            "Time for your medicine" if is_patient else "Medication due",
                            f"{med['name']}{(' ' + med['dose']) if med['dose'] else ''} is due now.",
                            payload,
                        )
                        notification_ids.append(note["id"])
                    notifications.append((recipients, payload, notification_ids))
                    add_log(conn, med["patient_id"], None, "med_due", f"Dose due: {med['name']}.")
                # Mark doses older than 2 hours as missed.
                conn.execute(
                    "UPDATE medication_events SET status='missed' WHERE status='pending' AND due_at < ?",
                    ((now - timedelta(hours=2)).isoformat(timespec="seconds"),),
                )
            for recipients, payload, notification_ids in notifications:
                for index, uid in enumerate(recipients):
                    emit(uid, "MEDICATION_DUE", {**payload, "notification_id": notification_ids[index]})
        except Exception as exc:  # pragma: no cover - worker must not die
            app.logger.warning("Scheduler error: %s", exc)
        time.sleep(30)


def delivery_worker():
    while True:
        try:
            cutoff = (datetime.now() - timedelta(minutes=2)).isoformat(timespec="seconds")
            escalations = []
            with db() as conn:
                pending = conn.execute(
                    "SELECT * FROM deliveries WHERE status='pending' AND created_at < ?", (cutoff,)
                ).fetchall()
                for delivery in pending:
                    attempts = delivery["attempts"] + 1
                    if attempts >= 3:
                        error = "Delivery unconfirmed. Browser push requires a native app/FCM."
                        conn.execute(
                            "UPDATE deliveries SET status='failed',attempts=?,last_error=?,updated_at=? WHERE id=?",
                            (attempts, error, now_iso(), delivery["id"]),
                        )
                    else:
                        conn.execute(
                            "UPDATE deliveries SET attempts=?,updated_at=? WHERE id=?",
                            (attempts, now_iso(), delivery["id"]),
                        )

                # Escalate unacknowledged SOS after 3 minutes to secondary contacts.
                old_sos = conn.execute(
                    "SELECT * FROM sos_events WHERE status='open' AND created_at < ?",
                    ((datetime.now() - timedelta(minutes=3)).isoformat(timespec="seconds"),),
                ).fetchall()
                for sos in old_sos:
                    already = conn.execute(
                        "SELECT 1 FROM deliveries WHERE event_type='sos' AND event_id=? AND channel='escalation'",
                        (sos["id"],),
                    ).fetchone()
                    if already:
                        continue
                    contacts = conn.execute(
                        "SELECT * FROM emergency_contacts WHERE patient_id=? AND verified=1", (sos["patient_id"],)
                    ).fetchall()
                    for contact in contacts:
                        create_delivery(
                            conn, "sos", sos["id"], "escalation", contact["id"],
                            f"Escalation: {contact['name']}", "pending",
                        )
                    add_log(conn, sos["patient_id"], None, "sos_escalated", "SOS escalated after no acknowledgement.")
                    escalations.append(sos["id"])
            for sos_id in escalations:
                with db() as conn:
                    row = conn.execute("SELECT patient_id FROM sos_events WHERE id=?", (sos_id,)).fetchone()
                if row:
                    for uid in stakeholder_ids_conn(row["patient_id"]):
                        emit(uid, "SOS_ESCALATED", {"sos_id": sos_id})
        except Exception as exc:  # pragma: no cover
            app.logger.warning("Delivery worker error: %s", exc)
        time.sleep(30)


threading.Thread(target=scheduler_worker, daemon=True, name="dose-scheduler").start()
threading.Thread(target=delivery_worker, daemon=True, name="delivery-worker").start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    print("ElderGuard MVP running at http://localhost:" + str(port))
    print("Database: " + DB_FILE)
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
