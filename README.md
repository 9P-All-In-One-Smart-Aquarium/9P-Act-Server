# Smart Aquarium Logic Server (AE-Logic)

This project is the **central control server** for a smart aquarium system based on the oneM2M platform (Mobius). The `AE-Logic` server acts as the "brain" that coordinates all interactions between ESP32 sensors, an AI server (Raspberry Pi), a mobile app, and the actuators.

This server is built using Python and the Flask framework. It leverages Mobius's Pub/Sub architecture to operate in a real-time, event-driven manner.

---

## 1. Core Architecture: Pub/Sub Central Control

This server follows a **Publish/Subscribe** model centered around Mobius (the CSE).

1.  **Publish:**
    * `AE-Sensor` (ESP32) `POST`s temperature, light, and water level data to Mobius.
    * `AE-Rapi` (AI Server) `POST`s fish health status (normal/disease) data.
    * `AE-App` (Mobile App) `POST`s thresholds, feeding times, pump, and the FCM token.
2.  **Subscribe:**
    * The `AE-Logic` server (this code) **subscribes to all Containers (CNTs)** listed in `SUBSCRIPTION_RESOURCE_URLS` upon startup.
3.  **Notification:**
    * Whenever new data (`cin`) is created in a subscribed `CNT`, Mobius immediately sends a `POST` notification to the `AE-Logic` server's `/notification` endpoint.
4.  **Process:**
    * The `AE-Logic` server receives the notification (`_process_notification_worker`) and translates the `pi` (parent resource ID) into a full path using the `g_ri_cache` map.
    * It parses the path to identify the `ae` and `cnt`, then executes the appropriate logic (e.g., `execute_sensor_logic`).
5.  **Control:**
    * Based on the logic's result, the server `POST`s `"on"`/`"off"` commands to the `AE-Actuator` (heater, LED) `CNT`s using the `send_mobius_command` function.

---

## 2. Key Features

### ① Real-time Automation Logic (`execute_sensor_logic`)

-   **Sensor-Threshold Comparison:** `temp`, `light`, and `wlevel` values are compared against the thresholds stored in `g_state`.
-   **Independent Control:** The logic for each actuator is triggered independently. For example, the heater logic runs only if the `temp` value is not `None`.
-   **Actuator Control:** Calls `send_mobius_command` to turn the heater/LED `"on"` or `"off"`.

### ② Background Scheduler (`scheduler_thread_fn`)

-   Runs in a **separate thread** from the Flask server, checking the time every 60 seconds.
-   Checks if the current time matches the `g_state["feeding_times"]` set by the app and if it's on the hour (`minute == 0`).
-   Sends an `"on"` command to the `feed` `CNT`, waits for 1 second (`time.sleep(1.0)`), and then sends an `"off"` command, ensuring the motor runs for exactly 1 second.

### ③ Notification Routing & Path Resolution (`_process_notification_worker`)

-   Mobius notifications often arrive with a **short resource ID (`ri`)** (e.g., `j59eumm1x0`) in the `pi` field.
-   The `resolve_resource_path` function and `g_ri_cache` map are used to translate this `ri` into a **full path** (e.g., `/Mobius/AE-Sensor/light`).
-   This allows the server to accurately route notifications by parsing the `ae` and `cnt` from the full path.

### ④ Robust Notification Handling (Debouncing & Thread-Safe)

-   **Debouncing:** The `schedule_debounce` function collects rapid-fire sensor alerts. If `temp` and `light` alerts arrive within 1 second, it waits and fires the `sensor_debounce_handler` **only once**, preventing server overload and redundant actuator commands.
-   **Thread-Safety:** Uses a `threading.Lock` (`g_lock`) to prevent race conditions when multiple threads try to modify global variables like `g_state` or `g_sensor_data_buffer` simultaneously.

### ⑤ State Persistence (Stateful)

-   The server is **stateful**. Thresholds, FCM tokens, and feeding times set by the app are stored in the `g_state` variable.
-   `save_state()` immediately writes these changes to `state_persistent.json`.
-   `load_state()` restores these settings from the file on server restart, ensuring no settings are lost.

### ⑥ External API Integration (FCM v1)

-   `init_fcm_auth` reads the `fcm-service-account.json` file to automatically handle Google OAuth 2.0 authentication.
-   `send_fcm_push` uses the obtained auth token to send push notifications to the mobile app via the modern **FCM HTTP v1 API**.

---

