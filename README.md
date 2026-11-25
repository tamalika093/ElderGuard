# ElderGuard — Care Beyond Distance

A **simple, role-based prototype** for remote family care:

1. Remote medication schedules that stay in sync.
2. One-tap instant family alerts.
3. SOS with live location shared with approved contacts and participating facilities.
4. A caregiver dashboard for medicines, alerts and emergency status.

This MVP is a deliberate simplification of the earlier hackathon demo. It removes
the confusing triple-screen presentation, hardcoded patient data, simulated
ambulance dispatch and fake lock screen. The work was developed on
`feature/elderguard-mvp` and merged into `main`.

---

## What changed from the old version

| Old problem | New behaviour |
|---|---|
| State lived only in memory; `STATE_FILE` was unused | SQLite database at `data/elderguard.db` is the source of truth. `data/state.json` is written as a readable debug snapshot. |
| Anyone could change anything; no auth | Real accounts with salted PBKDF2 password hashes and signed session cookies. Roles: patient, caregiver, provider. |
| Family access was assumed | Patients generate an invite code; caregivers request access; the patient explicitly approves it with permission flags. |
| Patient / contacts / hospital were hardcoded in `server.py` | Everything is created through onboarding forms. There are no seeded accounts and the default database is empty. |
| "Dispatch ambulance" pretended a real unit was sent | No dispatch endpoint exists. Facilities can only acknowledge or decline a **request**. Status never claims dispatch. |
| Lock screen was drawn inside the webpage | No fake lock screen. The UI clearly states browser limitations. |
| Browser siren implied locked-screen reliability | Banner and docs state that audio only works while the page is open. Native app + FCM is required for reliable background alerts. |
| One global SSE stream leaked all data | Per-user authenticated SSE streams. Caregivers only receive events for patients who approved access. |

---

## Quick start

```powershell
# From the project folder
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe server.py
```

Open <http://localhost:5000>.

Tailwind CSS is bundled at `static/tailwind.js`, so the dashboard works offline or when a CDN is blocked. A minimal `.hidden` fallback keeps screens switching correctly even if that file is missing.

### Try it quickly

1. Click **Create account** and register a patient, then a caregiver.
2. Use the patient's **Family access** card to generate an invite code and approve the caregiver's request.
3. Add medicines and try an instant alert. The patient account receives a notification and a toast/sound cue while its page is open.
4. Register a provider account with a facility name to activate that facility's request desk. Trigger an SOS from the patient account; the request appears in the provider's **Incoming emergency requests** list.
5. To start clean, delete `data/elderguard.db` and restart the server.

There are no demo accounts. Creating a provider account activates an in-app request desk for the named facility. This is prototype-level participation only; no ambulance service or external dispatch API is connected.

There are **no latitude/longitude fields in signup**. When an SOS is triggered, the app asks the browser for your location. If permission is denied or unavailable, the SOS is still sent and simply shows "Location not shared". Coordinates are used only to label a request as nearby; registered facility desks still receive the request record.

---

## How the MVP works

### Roles

- **Patient** — sees large medicine buttons, incoming alerts, SOS, medical ID, emergency contacts, family-access requests, and a notification inbox.
- **Caregiver** — selects an approved patient, adds medicines, sends instant alerts, watches emergency status, and manages a profile.
- **Provider** — registered to a facility request desk; sees incoming SOS requests, facility details, and request counts, and may acknowledge or decline them.

Every role has a **View / Edit profile** dialog in the header. Name, phone and city are editable; email and role are fixed after signup. Patients also have a separate **Medical ID** editor for blood group, allergies, conditions, doctor details and SOS sharing consent.

### Consent model

A caregiver cannot see or change anything until:

1. patient creates an invite code (`POST /api/family/invite`);
2. caregiver submits it (`POST /api/family/request`);
3. patient approves and chooses permissions (`POST /api/family/<id>/approve`).

Permissions are stored per relationship (`can_manage_meds`, `can_view_health`) and
checked on every patient-scoped endpoint.

#### How to use the patient invitation code (step by step)

1. **Patient signs in** and opens the **🔗 Family access** card.
2. Click **Generate invite code**. A short code appears (valid for 24 hours).
3. **Share that code** with the caregiver by WhatsApp/SMS/in person.
4. **Caregiver signs in** and opens the **👨‍👩‍👧 Family connection** card.
5. Type the code into **Enter patient's invite code** and click **Request**.
6. **Patient** sees the request in the same **Family access** card and clicks **Allow medicines** (or **Decline**).
7. Once approved, the patient appears in the caregiver's patient selector and the caregiver can add medicines and send alerts.

You do **not** type the code anywhere else. The patient *creates* it; the caregiver *enters* it; the patient *approves* it.

### Medicine reminders

- Schedules are stored in `medications`.
- When a caregiver adds a medicine, a persistent notification is stored for the patient and a toast/sound cue is pushed over SSE while the patient page is open.
- A background worker creates a `medication_events` row when a dose becomes due and stores another notification for the patient and approved caregivers.
- The patient taps **Taken** or **Snooze**. Snooze creates a new event 10 minutes later.
- Doses older than two hours are marked `missed`.
- Caregivers see the same status immediately over SSE.
- Browser audio and desktop notifications only work while the page is open and has been interacted with.

### Instant alerts

Types: Take Your Medicine, Please Check Your Phone, Health Check-In,
Emergency: Contact Me Immediately, and Custom. Every alert records delivery rows
so acknowledgement is visible instead of assumed.

### Notification inbox

The bell in the header opens a persistent inbox for medicine additions, due doses,
family alerts and SOS requests. Items stay unread until opened or marked read.
SSE adds an immediate toast and sound cue on top of the stored record; no push
notification is claimed when the browser is closed.

### SOS

- Requires a **2-second hold** in the UI plus a **60-second server cooldown** to reduce accidental triggers.
- Browser geolocation is used when available; otherwise the saved profile location is used and labelled as such.
- Every registered facility desk receives a request record. Matching facility/patient city or a distance within 25 km is added to the delivery label; it does not restrict the in-app inbox.
- Unverified emergency contacts are marked failed. Contact channels are records, not sent messages.
- A worker escalates unacknowledged SOS requests after three minutes.
- Providers may **acknowledge** or report **unable**. The UI never says "ambulance dispatched".
- The request detail view shares the patient's name, phone and city with authorized caregivers/providers. Medical ID details (blood group, allergies, conditions, doctor) are included only when the patient enables **Share in SOS**; otherwise the screen says they were not shared.

---

## Honest limitations (important for judging)

These cannot be fixed reliably in a plain browser and are intentionally documented:

1. **Background audio** — Web Audio only plays while the page is open and has been interacted with. A locked phone or closed tab cannot be guaranteed to ring. Production needs a native app with Firebase Cloud Messaging / high-priority push and OS-level alarm behaviour.
2. **True lock-screen button** — browsers cannot place a persistent button on the OS lock screen. Use Android/iOS shortcuts, widgets or a native app for that.
3. **SMS / real emergency services** — contact channels are recorded, not sent. Provider facilities receive an in-app request desk only; no partner dispatch API is connected.
4. **End-to-end encryption** — data is protected at rest in SQLite and in transit if deployed over HTTPS, but this prototype does not implement E2E encryption for medical records.
5. **Delivery confirmation** — in-app delivery is confirmed when the browser receives SSE. Push/SMS confirmation needs a real provider.

Do not present the prototype as dispatching ambulances or guaranteeing locked-device alarms.

---

## Suggested next improvements

1. Move to Flutter/React Native + FCM for reliable background notifications.
2. Replace SQLite with PostgreSQL/Firestore and add database-level row security.
3. Add real invite verification (SMS/OTP) for emergency contacts.
4. Sign a data-sharing agreement with participating facilities before enabling any external dispatch integration.
5. Add audit-log export and consent revocation.

---

## API overview

| Method | Path | Purpose |
|---|---|---|
| POST | `/api/auth/register`, `/api/auth/login`, `/api/auth/logout` | Accounts and sessions |
| GET | `/api/auth/me` | Current user |
| GET/PUT | `/api/me/profile` | View/edit account profile (all roles) |
| GET | `/api/notifications` | Persistent notification inbox |
| POST | `/api/notifications/<id>/read`, `/api/notifications/read-all` | Mark notifications read |
| POST | `/api/family/invite` | Patient creates invite code |
| POST | `/api/family/request` | Caregiver requests access |
| GET/POST | `/api/family/requests`, `/api/family/<id>/approve` | Consent workflow |
| GET | `/api/family/patients` | Caregiver's approved patients |
| GET/POST/PUT | `/api/patients/<pid>/profile`, `/emergency-contacts`, `/medications`, `/alerts`, `/sos`, `/activity` | Patient-scoped data |
| POST | `/api/medications/<id>/acknowledge` | Taken / snooze |
| POST | `/api/alerts/<id>/acknowledge` | Alert acknowledgement |
| POST | `/api/sos/<id>/acknowledge`, `/respond`, `/resolve` | Emergency status updates |
| GET | `/api/provider/requests` | Facility inbox |
| GET | `/api/events` | Authenticated per-user SSE |

Full details are in `server.py`. The old `README (2).md` is kept for reference but
describes the previous demo, not this simplified MVP.
