# ElderGuard - Remote Caregiver & Emergency Live Alert System

> **A real-time medical & emergency lifeline connecting children with aging parents across different cities.**

---

## 🌟 The Core Problem
Millions of working professionals live in different cities or states (e.g. Kolkata, Bangalore, Mumbai) while their aging parents live alone in their hometowns (e.g. Delhi, Patna, Siliguri).
- **The Medication Trap:** Seniors frequently forget to take vital BP, diabetes, or heart medications. Standard phone alarms are easily ignored, missed, or placed on silent.
- **The Panic Problem:** In acute emergencies (falls, chest pain, stroke, breathlessness), seniors cannot unlock phones, scroll contact lists, or make phone calls.

---

## 🚀 Unique Features & Hackathon USPs

1. **Remote Medication Trigger (Kolkata ➔ Delhi):**
   - The child in Kolkata can schedule doses and trigger **real-time high-decibel audible alarms** on their parents' phone in Delhi with personalized voice messages.
   - Built-in Web Audio API alarm siren + Text-to-Speech announcement (works in Bengali/English/Hindi tone).

2. **Two-Way Dose Acknowledgment Loop:**
   - Big touch targets for seniors: **[✓ Medicine Taken]** or **[⏰ Snooze 10m]**.
   - Immediate feedback sent back to Kolkata dashboard with time-stamped confirmation.
   - If ignored, caregivers are warned of missed doses.

3. **Lock Screen Emergency SOS Button:**
   - Seniors don't need to unlock their phone.
   - A single prominent red glowing SOS button triggers:
     - Blaring emergency siren.
     - Critical alert sent to family members.
     - Live emergency ticket dispatched to the nearest hospital trauma unit.
     - Displays **Emergency Medical ID** (Blood Group B+, Conditions, Allergies, Physician contact) on screen for first responders.

4. **Hospital & Emergency Dispatch Desk:**
   - Interactive OpenStreetMap/Leaflet integration showing patient location in CR Park, New Delhi.
   - One-click **Dispatch Ambulance** action with driver details and real-time ETA tracking.

5. **Triple-Screen Live Presentation Mode:**
   - Side-by-side view of all 3 parties (**Child in Kolkata | Parent in Delhi | Hospital Dispatch**) allowing hackathon judges to witness the entire cross-city communication loop in real-time on a single screen or across multiple devices.

---

## 🛠️ Tech Stack
- **Backend:** Python Flask + Server-Sent Events (SSE) for low-latency live event broadcast.
- **Frontend:** Responsive Single Page Application (HTML5, Tailwind CSS, Leaflet.js).
- **Audio Engine:** Synthetic Web Audio API oscillators (no external audio files required, zero latency) + Web Speech API synthesis.

---

## 🏃 Quick Start

1. Start the server:
   ```bash
   python server.py
   ```
2. Open your browser:
   ```
   http://localhost:5000
   ```
3. To test multi-device on mobile:
   - Connect phone to the same Wi-Fi.
   - Visit `http://<your-computer-ip>:5000` on your smartphone and switch to **"Parents Phone (Delhi)"** view!
