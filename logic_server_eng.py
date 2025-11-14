import os
import json
import time
import threading
import logging
from datetime import datetime
from typing import Dict, Any, Optional

import requests
from flask import Flask, request, jsonify

# Google libraries for FCM v1 (OAuth 2.0) authentication
import google.auth
import google.auth.transport.requests
from google.oauth2 import service_account

# ================================================================
# === Configuration ==============================================
# ================================================================

MOBIUS_URL = "http://localhost:7599"   # Base URL for the Mobius CSE
CSE_BASE = "Mobius"                    # The Base resource name of the Mobius CSE

# oneM2M resource tree structure definition (AE names)
AE_SENSOR = "AE-Sensor"
AE_APP = "AE-App"
AE_ACTUATOR = "AE-Actuator"
AE_RAPI = "AE-Rapi"

# (Critical) List of Mobius resources (CNTs) this server will subscribe to.
# When a `cin` is created in any of these CNTs, a notification will be sent to this server.
SUBSCRIPTION_RESOURCE_URLS = [
    # 1. Sensor CNTs (Individual subscriptions)
    f"{CSE_BASE}/{AE_SENSOR}/temp",
    f"{CSE_BASE}/{AE_SENSOR}/light",
    f"{CSE_BASE}/{AE_SENSOR}/wlevel",
    
    # 2. Threshold CNTs (Individual subscriptions)
    f"{CSE_BASE}/{AE_APP}/temp-threshold",
    f"{CSE_BASE}/{AE_APP}/light-threshold",
    f"{CSE_BASE}/{AE_APP}/wlevel-threshold",

    # 3. Other CNTs
    f"{CSE_BASE}/{AE_APP}/feed-time",     # Feeding schedule set by the app
    f"{CSE_BASE}/{AE_APP}/fcm-token",     # App's FCM device token
    f"{CSE_BASE}/{AE_ACTUATOR}/status", # Current status of the actuators (relays)
    f"{CSE_BASE}/{AE_RAPI}/status",     # Fish status from the Raspberry Pi AI
]

# Default HTTP headers for sending requests to Mobius
MOBIUS_HEADERS = {
    "X-M2M-Origin": "S-Logic",           # This server's AE-ID (Originator)
    "Content-Type": "application/json;ty=4" # Resource Type 4 = <contentInstance>
}

# --- FCM (Firebase Cloud Messaging) v1 (OAuth 2.0) Settings ---
# 1. The service account JSON file downloaded from the Firebase console
FCM_SERVICE_ACCOUNT_FILE = "fcm-service-account.json"
# 2. Your Firebase project's unique ID
FCM_PROJECT_ID = "app-9p"
# 3. The FCM v1 API endpoint
FCM_V1_ENDPOINT = f"https://fcm.googleapis.com/v1/projects/{FCM_PROJECT_ID}/messages:send"

# --- Debouncing Settings ---
# (To process multiple rapid notifications only once after a delay)
DEBOUNCE_DELAY_SENSOR_SEC = 1.0     # Collect sensor data (temp, light, wlevel) for 1 second
DEBOUNCE_DELAY_THRESHOLD_SEC = 2.0  # Collect threshold changes for 2 seconds

# --- Server State Persistence ---
# (File to remember settings like thresholds and FCM tokens even after a restart)
STATE_FILE = "state_persistent.json"

# --- Mobius Request Retry Settings ---
SEND_RETRY_COUNT = 3           # Retry up to 3 times on command failure
SEND_RETRY_DELAY_SEC = 1.0     # Wait 1 second between retries

# --- Flask Web Server Settings ---
FLASK_HOST = "0.0.0.0"  # (Allow access from any IP)
FLASK_PORT = 5000       # (The port to receive Mobius notifications)

# ================================================================
# === Globals ====================================================
# ================================================================

app = Flask(__name__) # Flask web server instance

# (Critical) A lock to safely access global variables in a multi-threaded environment.
# (Prevents notification handlers, the scheduler, and state savers from colliding)
g_lock = threading.Lock()

# (Critical) A cache to map Mobius's short resource IDs (`ri`) to full paths.
# (Mobius often sends the `ri` in the `pi` field instead of the full path)
g_ri_cache = {
    # --- AE-App ---
    "t0w2leo5xa": "/Mobius/AE-App/fcm-token",
    "do2p7vd28w": "/Mobius/AE-App/feed-time",
    "9us0f2ffxn": "/Mobius/AE-App/temp-threshold",
    "2k2mswswm5": "/Mobius/AE-App/light-threshold",
    "v57pjp881z": "/Mobius/AE-App/wlevel-threshold",

    # --- AE-Sensor ---
    "j59eumm1x0": "/Mobius/AE-Sensor/light",
    "34zc4w4j0h": "/Mobius/AE-Sensor/wlevel",
    "s1xp13tolq": "/Mobius/AE-Sensor/temp",

    # --- AE-Actuator ---
    "bb683ttsrt": "/Mobius/AE-Actuator/status",

    # --- AE-Rapi ---
    "i5542jzkuk": "/Mobius/AE-Rapi/status"
}

# (Critical) The server's "memory", storing its current state.
g_state = {
    "thresholds": {
        "temp-threshold": 20.0,
        "light-threshold": 100.0,
        "wlevel-threshold": 5.0,
    },
    "fcm_token": None,      # The app user's FCM device token
    "feeding_times": [],    # List of hours to feed (e.g., [9, 17])
}

# --- Temporary buffers for debouncing ---
g_sensor_data_buffer: Dict[str, Any] = {}     # (Temporarily holds sensor values)
g_threshold_data_buffer: Dict[str, Any] = {}  # (Temporarily holds threshold values)
g_timers: Dict[str, threading.Timer] = {}     # (Stores the active debouncing timer objects)

# --- Scheduler duplicate prevention ---
g_last_feed_hour = -1 # The last hour feeding was triggered (prevents duplicates)
last_rapa_status = None # The last Rapa (AI) status (prevents duplicate alerts)

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("logic_server_group")

# --- FCM Authentication Credentials (OAuth 2.0) ---
g_fcm_creds = None # (Filled by `init_fcm_auth` on server start)

# ================================================================
# === Persistence (Loading and Saving State) =====================
# ================================================================

# Loads the last state (`g_state`) from `STATE_FILE` on server start
def load_state():
    if not os.path.isfile(STATE_FILE):
        logger.info("No previous state file found.")
        return
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        with g_lock: # (Locking, as we are modifying g_state)
            g_state["thresholds"].update(data.get("thresholds", {}))
            g_state["fcm_token"] = data.get("fcm_token")
            g_state["feeding_times"] = data.get("feeding_times", [])
        logger.info("State loaded from %s", STATE_FILE)
    except Exception as e:
        logger.exception("Failed to load state: %s", e)

# Saves the current `g_state` to `STATE_FILE` as JSON (overwrite)
def save_state():
    with g_lock: # (Locking, as we are reading g_state)
        data = dict(g_state) # (Deadlock prevention: copy inside lock, do file I/O outside)
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.exception("Failed to save state: %s", e)

# ================================================================
# === Utility Helpers ============================================
# ================================================================

# Safely parses the `con` value from a `cin`, which could be a
# JSON string or a plain number/string.
def safe_parse_content(con):
    if isinstance(con, (dict, list)):
        return con
    try:
        return json.loads(con)
    except Exception:
        return str(con).strip() if con else ""

# (Critical) Resolves a resource's `pi` field. If it's an `ri` 
# (e.g., 'j59eumm1x0'), it's converted to a full path
# (e.g., '/Mobius/AE-Sensor/light') using the `g_ri_cache`.
def resolve_resource_path(ri: str) -> str:
    if not ri or ri.startswith("/"):
        return ri # (It's already a full path)
    with g_lock:
        if ri in g_ri_cache:
            return g_ri_cache[ri]
    logger.warning(f"resolve_resource_path: Unknown RI {ri}")
    return ri # (Cache miss, return the ri)

# (Critical) Sends a control command (`cin`) to Mobius via `POST`
def send_mobius_command(target_cnt_url: str, command: str) -> bool:
    for attempt in range(1, SEND_RETRY_COUNT + 1):
        headers = MOBIUS_HEADERS.copy()
        
        # (Important) `X-M2M-RI` (Request ID): This header is mandatory for Mobius.
        # We generate a unique one for each request to prevent `rqi is required` 400 errors.
        headers["X-M2M-RI"] = f"logic_req_{int(time.time() * 1000)}_{attempt}"
        
        try:
            payload = {"m2m:cin": {"con": command}}
            r = requests.post(
                target_cnt_url,
                json=payload,
                headers=headers,
                timeout=5,
            )
            if r.status_code in (200, 201):
                logger.info("Command success -> %s (cmd: %s)", target_cnt_url, command)
                return True
            else:
                logger.warning("Mobius POST failed %s: %s %s", attempt, r.status_code, r.text)
        except Exception as e:
            logger.warning("Exception sending Mobius POST: %s", e)
        time.sleep(SEND_RETRY_DELAY_SEC) # (Wait 1 sec before retrying)
    return False

# --- FCM v1 API (OAuth 2.0) Functions ---

# Loads the `fcm-service-account.json` file on server start
# to initialize `g_fcm_creds` (the FCM authentication object).
def init_fcm_auth():
    global g_fcm_creds
    try:
        g_fcm_creds = service_account.Credentials.from_service_account_file(
            FCM_SERVICE_ACCOUNT_FILE, 
            scopes=['https://www.googleapis.com/auth/firebase.messaging']
        )
        logger.info("FCM Service Account loaded successfully.")
    except FileNotFoundError:
        logger.error("="*50)
        logger.error(f"FCM service account file not found: {FCM_SERVICE_ACCOUNT_FILE}")
        logger.error("Step 1: Download your JSON key from the Firebase Console.")
        logger.error("="*50)
    except Exception as e:
        logger.exception("FCM auth initialization failed: %s", e)

# Sends a push notification to the app
def send_fcm_push(title, body, token=None):
    global g_fcm_creds
    with g_lock:
        tok = token or g_state.get("fcm_token")
    
    if not tok:
        logger.warning("FCM token not available. Skipping push notification.")
        return False
    
    if not g_fcm_creds:
        logger.warning("FCM auth not initialized. Skipping push notification.")
        return False
    
    # (FCM_PROJECT_ID must be set in the Configuration section)
    if FCM_PROJECT_ID == "YOUR_FIREBASE_PROJECT_ID":
        logger.warning("FCM_PROJECT_ID is not set. Skipping push notification.")
        return False

    try:
        # 1. Get an OAuth 2.0 access token (`g_fcm_creds` handles refresh automatically)
        auth_req = google.auth.transport.requests.Request()
        g_fcm_creds.refresh(auth_req)
        auth_token = g_fcm_creds.token

        headers = {
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/json",
        }

        # 2. The new FCM v1 API payload format
        payload = {
            "message": {
                "token": tok,
                "notification": {
                    "title": title,
                    "body": body
                }
            }
        }

        # 3. Send to the new v1 endpoint
        r = requests.post(FCM_V1_ENDPOINT, headers=headers, json=payload, timeout=5)
        
        if r.status_code == 200:
            logger.info(f"FCM push sent successfully (To: {tok[:10]}...): {r.text}")
            return True
        else:
            # (A 403 error means you need to enable the API in Google Cloud Console)
            logger.warning(f"FCM push failed. Status: {r.status_code}, Response: {r.text}")
            return False

    except Exception as e:
        logger.exception("FCM push exception: %s", e)
        return False

# (Critical) The debouncing scheduler.
# If called multiple times with the same `key` within the `delay` period,
# it cancels the previous timer and starts a new one.
def schedule_debounce(key: str, delay: float, callback, *args, **kwargs):
    with g_lock: # (Locking, as we are modifying g_timers)
        t = g_timers.get(key)
        if t:
            t.cancel() # (Cancel the previous timer, if it exists)
        
        # (Set a new timer to run the callback function after `delay` seconds)
        timer = threading.Timer(delay, callback, args, kwargs)
        g_timers[key] = timer
        timer.daemon = True # (Allow the program to exit even if timers are active)
        timer.start()

# ================================================================
# === Sensor / Threshold Logic (The "Brain") =====================
# ================================================================

# (Critical) The "Brain": Compares sensor values to thresholds and controls actuators.
def execute_sensor_logic(temp, light, wlevel):
    with g_lock:
        th = dict(g_state["thresholds"]) # (Safely copy the current thresholds)
        token = g_state["fcm_token"]
    try:
        t_thresh = th.get("temp-threshold")
        l_thresh = th.get("light-threshold")
        w_thresh = th.get("wlevel-threshold")
    except Exception as e:
        logger.error("Failed to parse single thresholds from g_state: %s. State: %s", e, th)
        return

    base_path = f"{MOBIUS_URL}/{CSE_BASE}/{AE_ACTUATOR}"
    
    # (Important) Each logic block runs independently, checking if its
    # sensor value is not None and is a valid number (not an empty string).
    
    # 1. Temperature Logic (Heater Control)
    if t_thresh is not None and isinstance(temp, (int, float)):
        if temp < t_thresh:
            logger.info(f"Temp {temp} < {t_thresh}, Heater ON")
            send_mobius_command(f"{base_path}/heater", "on")
        else: # (Greater than or equal to)
            logger.info(f"Temp {temp} >= {t_thresh}, Heater OFF")
            send_mobius_command(f"{base_path}/heater", "off")

    # 2. Light Logic (LED Control)
    if l_thresh is not None and isinstance(light, (int, float)):
        if light < l_thresh:
            logger.info(f"Light {light} < {l_thresh}, LED ON")
            send_mobius_command(f"{base_path}/LED", "on")
        else: # (Greater than or equal to)
            logger.info(f"Light {light} >= {l_thresh}, LED OFF")
            send_mobius_command(f"{base_path}/LED", "off")

    # 3. Water Level Logic (Push Notification Only)
    if w_thresh is not None and isinstance(wlevel, (int, float)):
        if wlevel < w_thresh:
            logger.info(f"W-Level {wlevel} < {w_thresh}, Sending Push")
            send_fcm_push("Water Level Low", f"Current water level: {wlevel}", token=token)

# The "callback function" executed when the `schedule_debounce` timer expires.
def sensor_debounce_handler():
    with g_lock:
        # (Copy the data collected in the buffer, then clear it)
        data = dict(g_sensor_data_buffer)
        g_sensor_data_buffer.clear()

    # (Get values from the buffer. If a sensor didn't report, its value is `None`)
    temp = data.get("temp")
    light = data.get("light")
    wlevel = data.get("wlevel")

    logger.info("Sensor group data: %s", data)
    
    # (Pass the data (including `None`s) to the main logic function)
    execute_sensor_logic(temp, light, wlevel)

# ================================================================
# === Notification Handlers ======================================
# ================================================================

# (Note: These functions are called by `_process_notification_worker`)

# (This is no longer used, as `_process_notification_worker` handles sensor logic directly)
def handle_sensor_group_notification(content):
    pass 

# (This is no longer used, as `_process_notification_worker` handles threshold logic directly)
def handle_threshold_group_notification(content):
    pass

# Handles notifications from the App (FCM token, feeding schedule)
def handle_app_notification(cnt, content):
    cnt = cnt.lower()
    if cnt == "feed-time":
        # (Handles various formats from the app, e.g., [9, 17] or "9, 17")
        parsed = safe_parse_content(content)
        times = []
        if isinstance(parsed, list):
            for t in parsed:
                try: times.append(int(t))
                except Exception: pass
        elif isinstance(parsed, str):
            for p in parsed.split(","):
                p = p.strip()
                if p.isdigit(): times.append(int(p))
        
        with g_lock:
            g_state["feeding_times"] = times
        save_state() # (Immediately save the state change to the file)
        logger.info("Feeding times set: %s", times)

    elif cnt == "fcm-token":
        with g_lock:
            g_state["fcm_token"] = str(content).strip()
        save_state() # (Immediately save the state change to the file)
        logger.info("FCM token updated.")
    else:
        logger.info("Unhandled APP cnt: %s", cnt)

# Handles notifications from the Rapa (AI)
def handle_rapa_notification(content):
    """Handles fish status codes (0, 1, 2) from Rapi."""
    global last_rapa_status
    try:
        status_code = int(safe_parse_content(content))
    except ValueError:
        logger.warning("Rapa status was not a valid integer: %s", content)
        return

    # (Important) If the status hasn't changed, ignore to prevent duplicate alerts.
    if last_rapa_status == status_code:
        logger.info("Rapa status unchanged; skipping push")
        return
    last_rapa_status = status_code  # (Update to the new status)

    title = None
    body = None

    if status_code == 1:
        title = "🐟 Aquarium Warning"
        body = "AI has detected a fish with potential 'tail nipping'."
        logger.info("Rapa status: 1 (Tail Nip). Pushing alert")
    elif status_code == 2:
        title = "🚨 Aquarium Emergency"
        body = "AI has detected a fish with potential 'white spot disease'. Please check immediately!"
        logger.info("Rapa status: 2 (White Spot). Pushing critical alert")
    elif status_code == 0:
        logger.info("Rapa status: 0 (Normal)")
        return
    else:
        logger.warning("Rapa status: Unknown code %d", status_code)
        return

    if title and body:
        with g_lock:
            token = g_state["fcm_token"]
        send_fcm_push(title, body, token=token)

# Handles status feedback from the Actuators
def handle_actuator_status(cnt, content):
    msg = f"{cnt} -> {content}"
    with g_lock:
        token = g_state["fcm_token"]
    send_fcm_push("Actuator Status", msg, token=token)

# ================================================================
# === Background worker + Flask endpoint (The "Heart") ===========
# ================================================================

# (Critical) The actual notification processing logic (runs in a background thread)
def _process_notification_worker(notification):
    logger.info("BG: start processing notification")
    try:
        nev = notification.get("nev")
        if not nev:
            logger.info("BG: no nev -> verification or ignore")
            return

        rep = nev.get("rep")
        if isinstance(rep, list): rep = rep[0] if rep else {}
        if not isinstance(rep, dict):
            logger.warning("BG: invalid rep structure: %s", rep)
            return

        # (Important) We subscribed with `nct=1`, so `m2m:cin` must exist.
        cin = rep.get("m2m:cin")
        if not cin:
            logger.info("BG: no m2m:cin -> ignored")
            return

        content = safe_parse_content(cin.get("con")) # (The data value)
        path = cin.get("pi", "") # (The parent's resource ID)

        # (Important) Convert `pi` (which might be an `ri`) to a full path
        if path and not path.startswith("/"):
            resolved = resolve_resource_path(path)
            if resolved != path:
                logger.info(f"Resolved resource ID {path} -> {resolved}")
                path = resolved

        # (Parse the path: /Mobius/AE-Sensor/temp -> ae=AE-Sensor, cnt=temp)
        parts = path.strip("/").split("/") if path else []
        ae = parts[1] if len(parts) > 1 else ""
        cnt = parts[-1] if parts else ""

        logger.info(f"BG handling: ae={ae}, cnt={cnt}, content={content}")

        # --- (Critical) Notification Routing ---
        
        if ae == AE_SENSOR:
            # (Sensor notifications are buffered and debounced for 1 sec)
            with g_lock:
                try:
                    g_sensor_data_buffer[cnt] = float(content)
                except Exception:
                    g_sensor_data_buffer[cnt] = content
            schedule_debounce("sensor", DEBOUNCE_DELAY_SENSOR_SEC, sensor_debounce_handler)
        
        elif ae == AE_APP:
            if "threshold" in cnt:
                # (Threshold notifications are buffered and debounced for 2 sec)
                with g_lock:
                    try:
                        g_threshold_data_buffer[cnt] = float(content)
                    except Exception:
                        logger.warning("Threshold content %s for %s is not a float", content, cnt)
                
                # (This 'commit' callback will run after the 2-sec timer)
                def commit():
                    with g_lock:
                        g_state["thresholds"].update(g_threshold_data_buffer)
                        logger.info("Thresholds updated (individual): %s", g_threshold_data_buffer)
                        g_threshold_data_buffer.clear()
                    save_state()
                schedule_debounce("threshold", DEBOUNCE_DELAY_THRESHOLD_SEC, commit)
            else:
                # (fcm-token, feed-time are processed immediately)
                handle_app_notification(cnt, content)

        elif ae == AE_RAPI:
            handle_rapa_notification(content)
        elif ae == AE_ACTUATOR:
            handle_actuator_status(cnt, content)
        else:
            logger.info("BG: Unhandled path: %s", path)
    except Exception as e:
        logger.exception("BG processing error: %s", e)
    finally:
        logger.info("BG: finished processing notification")

# (Critical) The Flask endpoint that receives notifications from Mobius
@app.route("/notification", methods=["POST"])
def notification_endpoint():
    try:
        data = request.get_json(force=True, silent=True)
    except Exception as ex:
        logger.exception("Invalid JSON: %s", ex)
        return jsonify({"rc": 400, "message": "invalid json"}), 400
    
    # (Check for the standard oneM2M notification wrapper)
    if not data or "m2m:sgn" not in data:
        return jsonify({"rc": 400, "message": "invalid notification format"}), 400

    notification = data["m2m:sgn"]

    # (Important) Delegate the actual work to a background thread
    # and immediately send `200 OK` back to Mobius.
    # (This prevents Mobius from timing out while waiting for a response)
    try:
        t = threading.Thread(target=_process_notification_worker, args=(notification,), daemon=True)
        t.start()
    except Exception as e:
        logger.exception("Failed to start background thread: %s", e)
        return jsonify({"rc": 200, "message": "ok (bg start failed)"}), 200

    return jsonify({"rc": 200, "message": "ok"}), 200

# ================================================================
# === Scheduler Thread (Background Scheduler) ====================
# ================================================================

# (Critical) The automatic feeding scheduler (runs in a separate thread)
def scheduler_thread_fn():
    global g_last_feed_hour
    while True:
        try:
            now = datetime.now()
            hour, minute = now.hour, now.minute
            
            with g_lock:
                times = list(g_state["feeding_times"]) # (Get the schedule from state)
                token = g_state["fcm_token"]

            # (Condition: Is it on the hour (minute=0)? Is this hour in the schedule?
            #  And have we *not* already fed this hour?)
            if minute == 0 and hour in times and g_last_feed_hour != hour:
                
                target_url = f"{MOBIUS_URL}/{CSE_BASE}/{AE_ACTUATOR}/feed"
                logger.info(f"Scheduler: Executing feed command at {hour}:{minute}")
                
                # (1) Send "on" command
                success = send_mobius_command(target_url, "on")
                
                if success:
                    send_fcm_push("Feeding", f"Feeding executed at {hour}:00", token=token)
                    
                    # (Important) Wait for 1 second (motor run time)
                    logger.info("Scheduler: Waiting 1 seconds to send OFF...")
                    time.sleep(1.0) # (Adjust this value to change motor run time)

                    # (2) Send "off" command
                    logger.info("Scheduler: Sending OFF command...")
                    send_mobius_command(target_url, "off")
                else:
                    send_fcm_push("Feeding Failed", f"Feeding failed at {hour}:00", token=token)
                
                g_last_feed_hour = hour # (Mark this hour as "done" to prevent duplicates)
            
            # (Reset the flag when it's no longer the 0th minute)
            if minute != 0:
                g_last_feed_hour = -1

        except Exception as e:
            logger.warning("Scheduler error: %s", e)
        
        # (This loop repeats every 60 seconds)
        time.sleep(60)

# ================================================================
# === Subscription Creation ======================================
# ================================================================

# (Critical) Creates the subscriptions on Mobius when the server starts
def check_and_create_subscription():
    logger.info("Creating subscriptions for group resources...")
    for target in SUBSCRIPTION_RESOURCE_URLS:

        # (Note: The `rn` (resource name) should ideally be unique for each subscription)
        # (The line below is commented out but is best practice)
        sub_name = f"sub-logic-{target.split('/')[-1]}"
        
        payload = {
            "m2m:sub": {
                # (Best practice: change "sub-logic" to `sub_name` to avoid 409 conflicts)
                "rn": "sub-logic", 
                "nu": [f"http://localhost:{FLASK_PORT}/notification"], # (URL to send alerts to)
                
                # (Important) nct=1: Include the full `m2m:cin` object in the notification
                "nct": 1, 
                
                # (Important) enc.net=3: Notify only on *Creation* of a Child Resource (a `cin`)
                "enc": {"net":[3]} 
            }
        }
        try:
            # (Must use resource type 23 (ty=23) to create a <sub>)
            sub_headers = MOBIUS_HEADERS.copy()
            sub_headers["Content-Type"] = "application/json;ty=23"
            
            # (Add the mandatory unique `X-M2M-RI` header)
            sub_headers["X-M2M-RI"] = f"logic_sub_{int(time.time() * 1000)}_{target.split('/')[-1]}"

            target_url = f"{MOBIUS_URL}/{target}"
            r = requests.post(target_url, json=payload, headers=sub_headers, timeout=5)
            
            if r.status_code in (200, 201):
                logger.info("Subscribed to %s", target)
            elif r.status_code == 409: # (409 Conflict = already exists)
                logger.info("Subscription already exists for %s", target)
            else:
                logger.warning("Sub status %s: %s", r.status_code, r.text)
        except Exception as e:
            logger.warning("Subscription error for %s: %s", target, e)

# ================================================================
# === Main Entry (Server Start) ==================================
# ================================================================
def start_server():
    # 1. Restore previous state from `state_persistent.json`
    load_state()
    
    # 2. Initialize FCM v1 authentication
    init_fcm_auth()
    
    # 3. Start the background feeding scheduler thread
    threading.Thread(target=scheduler_thread_fn, daemon=True).start()
    
    # 4. Create notification subscriptions on Mobius
    check_and_create_subscription()
    
    # 5. Start the Flask web server (to receive notifications)
    # (threaded=True allows handling multiple notifications simultaneously)
    app.run(host=FLASK_HOST, port=FLASK_PORT, threaded=True)

if __name__ == "__main__":
    start_server()