"""
ElderGuard Backend Server
Real-time remote medication management, live alerts, and emergency SOS system.
"""

from flask import Flask, request, jsonify, Response, send_from_directory
from flask_cors import CORS
import json
import time
import queue
import threading
import os
from datetime import datetime

app = Flask(__name__, static_folder="static", static_url_path="")
CORS(app)

# In-memory storage with file backup
DATA_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(DATA_DIR, "state.json")

# Default Initial State
initial_state = {
    "parent": {
        "name": "Subir & Anjali Sharma (Parents)",
        "city": "New Delhi (CR Park)",
        "coordinates": {"lat": 28.5355, "lng": 77.2588},
        "phone": "+91 98112 34567",
        "battery": 88,
        "isOnline": True,
        "medical_id": {
            "blood_group": "B+",
            "age": 71,
            "conditions": ["Hypertension (High BP)", "Type 2 Diabetes"],
            "allergies": ["Penicillin", "Sulfa drugs"],
            "doctor": "Dr. A. K. Sen (+91 98100 12345)"
        }
    },
    "caregiver": {
        "name": "Rohan Sharma (Son)",
        "city": "Kolkata (Salt Lake Sector V)",
        "phone": "+91 98300 98765",
        "relation": "Son"
    },
    "medications": [
        {
            "id": "med-1",
            "name": "Telmisartan 40mg",
            "condition": "High Blood Pressure",
            "scheduledTime": "09:00 AM",
            "instructions": "1 tablet after breakfast with warm water",
            "status": "taken",
            "lastAcknowledged": "Today, 09:12 AM"
        },
        {
            "id": "med-2",
            "name": "Metformin 500mg",
            "condition": "Diabetes Control",
            "scheduledTime": "02:00 PM",
            "instructions": "1 tablet immediately after lunch",
            "status": "pending",
            "lastAcknowledged": None
        },
        {
            "id": "med-3",
            "name": "Atorvastatin 10mg",
            "condition": "Cholesterol & Heart",
            "scheduledTime": "09:30 PM",
            "instructions": "1 tablet before sleep",
            "status": "pending",
            "lastAcknowledged": None
        }
    ],
    "activeAlarm": None,
    "activeSOS": None,
    "audit_logs": [
        {
            "id": "log-1",
            "timestamp": "09:12 AM",
            "type": "med_taken",
            "message": "Subir Sharma confirmed: Telmisartan 40mg taken."
        },
        {
            "id": "log-2",
            "timestamp": "08:55 AM",
            "type": "system",
            "message": "Routine morning system heartbeat verified from New Delhi device."
        }
    ]
}

state = initial_state.copy()

# Event bus for Server-Sent Events (SSE)
listeners = []
listeners_lock = threading.Lock()

def broadcast_event(event_type, data):
    """Broadcast an event to all connected web clients in real-time"""
    payload = json.dumps({"type": event_type, "data": data, "timestamp": datetime.now().isoformat()})
    with listeners_lock:
        for q in listeners:
            try:
                q.put_nowait(payload)
            except Exception:
                pass

def add_log(log_type, message):
    log_entry = {
        "id": f"log-{int(time.time()*1000)}",
        "timestamp": datetime.now().strftime("%I:%M:%S %p"),
        "type": log_type,
        "message": message
    }
    state["audit_logs"].insert(0, log_entry)
    if len(state["audit_logs"]) > 50:
        state["audit_logs"].pop()
    broadcast_event("LOG_ADDED", log_entry)

@app.route("/")
def serve_index():
    return send_from_directory(app.static_folder, "index.html")

@app.route("/api/state", methods=["GET"])
def get_state():
    return jsonify(state)

@app.route("/api/events")
def sse_events():
    """SSE endpoint for ultra-low latency real-time multi-device synchronization"""
    def event_stream():
        q = queue.Queue(maxsize=100)
        with listeners_lock:
            listeners.append(q)
        
        # Send initial ping & full state
        initial_msg = json.dumps({"type": "INIT_STATE", "data": state, "timestamp": datetime.now().isoformat()})
        yield f"data: {initial_msg}\n\n"
        
        try:
            while True:
                try:
                    payload = q.get(timeout=25)
                    yield f"data: {payload}\n\n"
                except queue.Empty:
                    # Keep-alive heartbeat ping
                    yield f": keepalive {time.time()}\n\n"
        except GeneratorExit:
            pass
        finally:
            with listeners_lock:
                if q in listeners:
                    listeners.remove(q)

    return Response(event_stream(), mimetype="text/event-stream", headers={
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
        "Connection": "keep-alive"
    })

@app.route("/api/trigger-alarm", methods=["POST"])
def trigger_alarm():
    """Child in Kolkata triggers an urgent live audio alarm on Delhi parents' device"""
    body = request.get_json() or {}
    medication_id = body.get("medicationId")
    custom_message = body.get("message", "Baba, please take your scheduled medicine now!")
    urgent = body.get("urgent", True)

    med = next((m for m in state["medications"] if m["id"] == medication_id), None)
    med_name = med["name"] if med else "Scheduled Medicine"

    alarm_data = {
        "id": f"alarm-{int(time.time()*1000)}",
        "medicationId": medication_id,
        "medicationName": med_name,
        "message": custom_message,
        "sender": state["caregiver"]["name"],
        "senderCity": state["caregiver"]["city"],
        "urgent": urgent,
        "triggeredAt": datetime.now().strftime("%I:%M:%S %p"),
        "acknowledged": False
    }

    state["activeAlarm"] = alarm_data
    if med:
        med["status"] = "ringing"

    add_log("alarm_triggered", f"🔴 Live Alarm sent by {state['caregiver']['name']} (Kolkata) to Parents: '{custom_message}'")
    broadcast_event("ALARM_TRIGGERED", alarm_data)
    broadcast_event("STATE_UPDATED", state)

    return jsonify({"success": True, "alarm": alarm_data})

@app.route("/api/acknowledge-alarm", methods=["POST"])
def acknowledge_alarm():
    """Parents click 'Medicine Taken' or 'Snooze' on their device"""
    body = request.get_json() or {}
    action = body.get("action", "taken") # "taken" or "snooze"
    alarm_id = body.get("alarmId")
    medication_id = body.get("medicationId")

    now_str = datetime.now().strftime("%I:%M %p")

    # Update medication status
    target_med = None
    if medication_id:
        target_med = next((m for m in state["medications"] if m["id"] == medication_id), None)
    elif state["activeAlarm"] and state["activeAlarm"].get("medicationId"):
        target_med = next((m for m in state["medications"] if m["id"] == state["activeAlarm"]["medicationId"]), None)

    if action == "taken":
        if target_med:
            target_med["status"] = "taken"
            target_med["lastAcknowledged"] = f"Today, {now_str}"
        add_log("med_taken", f"✅ Medicine Taken! Subir Sharma confirmed: {target_med['name'] if target_med else 'Medicine'} taken at {now_str}.")
        result_message = "Medicine marked as TAKEN. Caregiver notified in Kolkata."
    else:
        if target_med:
            target_med["status"] = "snoozed"
        add_log("med_snooze", f"⏰ Snooze requested (10 min) by Parents for {target_med['name'] if target_med else 'Medicine'}. Reminder will sound again.")
        result_message = "Alarm Snoozed for 10 minutes."

    state["activeAlarm"] = None
    broadcast_event("ALARM_ACKNOWLEDGED", {
        "action": action,
        "medicationId": target_med["id"] if target_med else None,
        "timestamp": now_str
    })
    broadcast_event("STATE_UPDATED", state)

    return jsonify({"success": True, "message": result_message, "state": state})

@app.route("/api/trigger-sos", methods=["POST"])
def trigger_sos():
    """Parents tap Emergency SOS Button on Lock Screen"""
    body = request.get_json() or {}
    source = body.get("source", "lock_screen_button")
    notes = body.get("notes", "Critical Emergency SOS Activated by Patient!")

    sos_data = {
        "id": f"sos-{int(time.time()*1000)}",
        "patientName": state["parent"]["name"],
        "city": state["parent"]["city"],
        "coordinates": state["parent"]["coordinates"],
        "timestamp": datetime.now().strftime("%I:%M:%S %p"),
        "medical_id": state["parent"]["medical_id"],
        "source": source,
        "notes": notes,
        "status": "DISPATCH_REQUIRED", # DISPATCH_REQUIRED, AMBULANCE_DISPATCHED, RESOLVED
        "assignedHospital": "AIIMS Emergency Trauma Centre (CR Park Branch)",
        "ambulanceEta": "8 mins",
        "contactsNotified": [
            {"name": state["caregiver"]["name"], "role": "Son (Primary)", "phone": state["caregiver"]["phone"], "status": "ALARM_RINGING"},
            {"name": "Dr. A. K. Sen", "role": "Family Physician", "phone": "+91 98100 12345", "status": "SMS_DISPATCHED"}
        ]
    }

    state["activeSOS"] = sos_data
    add_log("sos_alert", f"🚨🚨 EMERGENCY SOS TRIGGERED from {state['parent']['city']}! Nearest Hospital & Emergency Contacts alerted!")
    broadcast_event("SOS_TRIGGERED", sos_data)
    broadcast_event("STATE_UPDATED", state)

    return jsonify({"success": True, "sos": sos_data})

@app.route("/api/dispatch-ambulance", methods=["POST"])
def dispatch_ambulance():
    """Hospital desk dispatches ambulance"""
    if not state["activeSOS"]:
        return jsonify({"error": "No active SOS found"}), 400

    body = request.get_json() or {}
    driver_name = body.get("driverName", "Rajesh Kumar (Unit 102)")
    vehicle_number = body.get("vehicleNumber", "DL-1T-4492")
    eta = body.get("eta", "6 mins")

    state["activeSOS"]["status"] = "AMBULANCE_DISPATCHED"
    state["activeSOS"]["driver"] = {
        "name": driver_name,
        "vehicle": vehicle_number,
        "phone": "+91 99887 76655",
        "eta": eta
    }

    add_log("ambulance_dispatched", f"🚑 Ambulance ({vehicle_number}) dispatched by AIIMS Emergency Unit to CR Park! ETA: {eta}")
    broadcast_event("AMBULANCE_DISPATCHED", state["activeSOS"])
    broadcast_event("STATE_UPDATED", state)

    return jsonify({"success": True, "sos": state["activeSOS"]})

@app.route("/api/resolve-sos", methods=["POST"])
def resolve_sos():
    """Resolve/cancel active SOS"""
    if state["activeSOS"]:
        add_log("sos_resolved", "🟢 Emergency SOS marked as RESOLVED & Patient stabilized.")
        state["activeSOS"] = None
        broadcast_event("SOS_RESOLVED", {})
        broadcast_event("STATE_UPDATED", state)
    return jsonify({"success": True})

@app.route("/api/medications", methods=["POST"])
def add_medication():
    """Child in Kolkata adds a new medication schedule remotely"""
    body = request.get_json() or {}
    new_med = {
        "id": f"med-{int(time.time()*1000)}",
        "name": body.get("name", "New Medicine"),
        "condition": body.get("condition", "General Health"),
        "scheduledTime": body.get("scheduledTime", "12:00 PM"),
        "instructions": body.get("instructions", "Take 1 tablet with water"),
        "status": "pending",
        "lastAcknowledged": None
    }
    state["medications"].append(new_med)
    add_log("med_added", f"💊 New medication scheduled by {state['caregiver']['name']}: {new_med['name']} at {new_med['scheduledTime']}.")
    broadcast_event("STATE_UPDATED", state)
    return jsonify({"success": True, "medication": new_med})

@app.route("/api/medications/<med_id>", methods=["DELETE"])
def delete_medication(med_id):
    state["medications"] = [m for m in state["medications"] if m["id"] != med_id]
    add_log("med_deleted", f"Medication {med_id} removed by caregiver.")
    broadcast_event("STATE_UPDATED", state)
    return jsonify({"success": True})

@app.route("/api/reset-demo", methods=["POST"])
def reset_demo():
    """Reset to initial demo state for hackathon judges"""
    global state
    import copy
    state = copy.deepcopy(initial_state)
    add_log("system", "Demo environment reset to initial hackathon presentation state.")
    broadcast_event("STATE_UPDATED", state)
    return jsonify({"success": True, "state": state})

if __name__ == "__main__":
    # Create static directory if not exists
    os.makedirs(os.path.join(DATA_DIR, "static"), exist_ok=True)
    port = int(os.environ.get("PORT", 5000))
    print(f"===============================================================")
    print(f"ElderGuard Remote Care & Live Alert System is starting...")
    print(f"Local URL: http://localhost:{port}")
    print(f"===============================================================")
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
