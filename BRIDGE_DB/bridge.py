"""
bridge.py — Railway
-------------------
Suscriptor MQTT que persiste en PostgreSQL.

Por cada mensaje recibido en universidad/jaen/{sensor_id}/{variable}:
  1. Inserta en `telemetria`   (raw log, como antes)
  2. Inserta en `sfa_readings` (EAV limpia)
  3. Evalúa umbrales e inserta en `sfa_alerts` si procede
"""

import json
import os
import time
from datetime import datetime, timezone

import psycopg2
import paho.mqtt.client as mqtt

# ==========================================
# CONFIGURACIÓN DESDE VARIABLES DE ENTORNO
# ==========================================
DB_URL      = os.getenv("DATABASE_URL")
MQTT_BROKER = os.getenv("MQTT_HOST", "mqtt-server")
MQTT_PORT   = int(os.getenv("MQTT_PORT", 1883))
TOPIC_SUB   = "universidad/jaen/#"
TOPIC_BASE  = "universidad/jaen"   # para parsear sensor_id y variable

# ==========================================
# VARIABLES SFA CONOCIDAS
# ==========================================
KNOWN_VARIABLES = {
    "radiacion_solar",
    "temperatura_ambiente",
    "corriente_generada",
    "tension_bateria",
    "corriente_bateria",
    "corriente_carga",
    "temperatura_bateria",
}

# ==========================================
# UMBRALES DE ALERTA
# ==========================================
ALERT_RULES = [
    {"variable": "tension_bateria",     "threshold": 11.8, "operator": "<=",
     "level": "warning", "message": "Tensión baja: {value} V"},
    {"variable": "temperatura_bateria", "threshold": 45.0, "operator": ">=",
     "level": "warning", "message": "Temperatura alta: {value} °C"},
]

# ==========================================
# CONEXIÓN A POSTGRESQL CON REINTENTOS
# ==========================================
conn   = None
cursor = None

print("🚀 Iniciando Bridge MQTT → Postgres...")

while True:
    try:
        print("🔗 Conectando a la DB...")
        conn   = psycopg2.connect(DB_URL)
        cursor = conn.cursor()

        # --- Tabla raw (existente) ---
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS telemetria (
                id      SERIAL PRIMARY KEY,
                topic   TEXT,
                payload JSONB,
                fecha   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # --- Tabla limpia EAV ---
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sfa_readings (
                id            BIGSERIAL        PRIMARY KEY,
                timestamp     TIMESTAMPTZ      NOT NULL,
                sensor_id     VARCHAR(64)      NOT NULL,
                variable      VARCHAR(64)      NOT NULL,
                value         DOUBLE PRECISION NOT NULL,
                source        VARCHAR(20)      DEFAULT 'mqtt',
                telemetria_id BIGINT
            );

            CREATE INDEX IF NOT EXISTS idx_sfa_readings_sensor_var_ts
                ON sfa_readings (sensor_id, variable, timestamp DESC);
        """)

        # --- Tabla de alertas ---
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sfa_alerts (
                id         BIGSERIAL   PRIMARY KEY,
                reading_id BIGINT      REFERENCES sfa_readings(id) ON DELETE CASCADE,
                timestamp  TIMESTAMPTZ NOT NULL,
                sensor_id  VARCHAR(64) NOT NULL,
                level      VARCHAR(10) NOT NULL,
                variable   VARCHAR(64) NOT NULL,
                message    TEXT        NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_sfa_alerts_sensor_ts
                ON sfa_alerts (sensor_id, timestamp DESC);
        """)

        conn.commit()
        print("✅ Base de datos conectada y tablas listas.")
        break

    except Exception as e:
        print(f"⏳ Postgres no disponible, reintentando en 5s... ({e})")
        time.sleep(5)


# ==========================================
# HELPERS
# ==========================================
def _parse_topic(topic: str) -> tuple[str, str] | tuple[None, None]:
    """Extrae (sensor_id, variable) de universidad/jaen/{sensor_id}/{variable}."""
    prefix = TOPIC_BASE + "/"
    if not topic.startswith(prefix):
        return None, None
    parts = topic[len(prefix):].split("/")
    if len(parts) != 2:
        return None, None
    return parts[0], parts[1]


def _insert_telemetria(topic: str, payload_dict: dict) -> int:
    cursor.execute(
        "INSERT INTO telemetria (topic, payload) VALUES (%s, %s) RETURNING id",
        (topic, json.dumps(payload_dict))
    )
    return cursor.fetchone()[0]


def _insert_reading(sensor_id: str, variable: str, value: float,
                    timestamp: datetime, source: str, telemetria_id: int) -> int:
    cursor.execute("""
        INSERT INTO sfa_readings
            (timestamp, sensor_id, variable, value, source, telemetria_id)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id
    """, (timestamp, sensor_id, variable, value, source, telemetria_id))
    return cursor.fetchone()[0]


def _insert_alerts(reading_id: int, sensor_id: str,
                   variable: str, value: float, timestamp: datetime):
    for rule in ALERT_RULES:
        if rule["variable"] != variable:
            continue
        fired = (value <= rule["threshold"] if rule["operator"] == "<="
                 else value >= rule["threshold"])
        if not fired:
            continue
        cursor.execute("""
            INSERT INTO sfa_alerts
                (reading_id, timestamp, sensor_id, level, variable, message)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            reading_id,
            timestamp,
            sensor_id,
            rule["level"],
            variable,
            rule["message"].format(value=round(value, 2)),
        ))
        print(f"  ⚠️  [{sensor_id}] {rule['level'].upper()} — "
              f"{rule['message'].format(value=round(value, 2))}")


# ==========================================
# CALLBACKS MQTT
# ==========================================
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print(f"✅ Conectado al Broker MQTT interno ({MQTT_BROKER})")
        client.subscribe(TOPIC_SUB)
        print(f"📡 Suscrito a: {TOPIC_SUB}")
    else:
        print(f"❌ Error de conexión MQTT. Código: {rc}")


def on_message(client, userdata, msg):
    try:
        raw = msg.payload.decode()
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            payload = {"raw_text": raw}

        # 1. Insertar en telemetria (siempre)
        tel_id = _insert_telemetria(msg.topic, payload)

        # 2. Parsear topic para obtener sensor_id y variable
        sensor_id, variable = _parse_topic(msg.topic)

        if sensor_id and variable and variable in KNOWN_VARIABLES:
            # 3. Extraer value y timestamp
            try:
                value = float(payload.get("value") or payload.get(variable))
            except (TypeError, ValueError):
                print(f"⚠️  No se pudo extraer valor de {msg.topic}")
                conn.commit()
                return

            ts_raw = payload.get("timestamp")
            if ts_raw:
                ts = datetime.fromisoformat(ts_raw)
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
            else:
                ts = datetime.now(timezone.utc)

            source = payload.get("source", "mqtt")

            # 4. Insertar en sfa_readings
            reading_id = _insert_reading(sensor_id, variable, value, ts, source, tel_id)

            # 5. Evaluar alertas
            _insert_alerts(reading_id, sensor_id, variable, value, ts)

            print(f"📥 [{sensor_id}] {variable} = {value}  (tel_id={tel_id})")
        else:
            print(f"📥 [raw] {msg.topic} guardado en telemetria (id={tel_id})")

        conn.commit()

    except Exception as e:
        print(f"❌ Error al procesar mensaje: {e}")
        if conn:
            conn.rollback()


# ==========================================
# CONEXIÓN MQTT CON REINTENTOS
# ==========================================
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_connect = on_connect
client.on_message = on_message

while True:
    try:
        print(f"📡 Conectando a MQTT en {MQTT_BROKER}:{MQTT_PORT}...")
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        break
    except Exception as e:
        print(f"⏳ Broker no disponible, reintentando en 5s... ({e})")
        time.sleep(5)

try:
    client.loop_forever()
except KeyboardInterrupt:
    print("🛑 Bridge detenido.")
finally:
    if cursor: cursor.close()
    if conn:   conn.close()