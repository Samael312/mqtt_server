import json
import os
import time
from datetime import datetime, timezone

import psycopg2
from psycopg2 import errors
import paho.mqtt.client as mqtt

# ==========================================
# CONFIGURACIÓN
# ==========================================
DB_URL      = os.getenv("DATABASE_URL")
MQTT_BROKER = os.getenv("MQTT_HOST", "mqtt-server")
MQTT_PORT   = int(os.getenv("MQTT_PORT", 1883))
TOPIC_SUB   = "universidad/jaen/#"
TOPIC_BASE  = "universidad/jaen"

KNOWN_VARIABLES = {
    "radiacion_solar", "temperatura_ambiente", "corriente_generada",
    "tension_bateria", "corriente_bateria", "corriente_carga",
    "temperatura_bateria",
}

# ==========================================
# LÓGICA DE BASE DE DATOS (AUTO-REPARABLE)
# ==========================================
def init_db(conn):
    """Crea la estructura de tablas si no existe."""
    with conn.cursor() as cursor:
        print("🛠️ Verificando/Creando estructura de tablas...")
        
        # 1. Tabla Telemetría
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS telemetria (
                id      SERIAL PRIMARY KEY,
                topic   TEXT,
                payload JSONB,
                fecha   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # 2. Tabla SFA Readings
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

        # 3. Tabla Alertas
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sfa_alerts (
                id         BIGSERIAL        PRIMARY KEY,
                reading_id BIGINT           REFERENCES sfa_readings(id) ON DELETE CASCADE,
                timestamp  TIMESTAMPTZ      NOT NULL,
                sensor_id  VARCHAR(64)      NOT NULL,
                level      VARCHAR(10)      NOT NULL,
                variable   VARCHAR(64)      NOT NULL,
                value      DOUBLE PRECISION NOT NULL,
                message    TEXT             NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_sfa_alerts_sensor_ts
                ON sfa_alerts (sensor_id, timestamp DESC);
        """)

        # 4. Tabla Reglas
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS alert_rules (
                id         BIGSERIAL    PRIMARY KEY,
                sensor_id  VARCHAR(64)  NOT NULL,
                variable   VARCHAR(64)  NOT NULL,
                operator   VARCHAR(2)   NOT NULL,
                threshold  DOUBLE PRECISION NOT NULL,
                level      VARCHAR(10)  NOT NULL DEFAULT 'warning',
                message    TEXT         NOT NULL,
                created_at TIMESTAMPTZ  DEFAULT NOW(),
                UNIQUE (sensor_id, variable, operator)
            );
            CREATE INDEX IF NOT EXISTS idx_alert_rules_sensor ON alert_rules (sensor_id);
        """)
        conn.commit()
        print("✅ Estructura lista.")

def get_connection():
    """Establece conexión con reintentos."""
    while True:
        try:
            conn = psycopg2.connect(DB_URL)
            return conn
        except Exception as e:
            print(f"⏳ DB no disponible, reintentando... ({e})")
            time.sleep(5)

# Conexión inicial
conn = get_connection()
init_db(conn)

# ==========================================
# CALLBACKS MQTT
# ==========================================
def on_message(client, userdata, msg):
    global conn
    try:
        raw = msg.payload.decode()
        payload = json.loads(raw) if raw.startswith('{') else {"raw_text": raw}
        
        with conn.cursor() as cursor:
            # 1. Insertar Telemetría
            cursor.execute(
                "INSERT INTO telemetria (topic, payload) VALUES (%s, %s) RETURNING id",
                (msg.topic, json.dumps(payload))
            )
            tel_id = cursor.fetchone()[0]

            # 2. Procesar variables SFA
            prefix = TOPIC_BASE + "/"
            if msg.topic.startswith(prefix):
                parts = msg.topic[len(prefix):].split("/")
                if len(parts) == 2:
                    sensor_id, variable = parts
                    if variable in KNOWN_VARIABLES:
                        value = float(payload.get("value") or payload.get(variable, 0))
                        
                        ts_raw = payload.get("timestamp")
                        ts = datetime.fromisoformat(ts_raw) if ts_raw else datetime.now(timezone.utc)
                        if ts.tzinfo is None: ts = ts.replace(tzinfo=timezone.utc)

                        cursor.execute("""
                            INSERT INTO sfa_readings (timestamp, sensor_id, variable, value, telemetria_id)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (ts, sensor_id, variable, value, tel_id))
                        print(f"📥 [{sensor_id}] {variable}={value}")

            conn.commit()

    except (psycopg2.InterfaceError, psycopg2.OperationalError):
        print("🔌 Conexión perdida con DB. Reconectando...")
        conn = get_connection()
    
    except errors.UndefinedTable:
        print("⚠️ ¡TABLAS BORRADAS! Reconstruyendo en caliente...")
        conn.rollback()
        init_db(conn) # <--- Aquí ocurre la magia de la autoreparación
        
    except Exception as e:
        print(f"❌ Error crítico: {e}")
        conn.rollback()

# ==========================================
# MAIN LOOP
# ==========================================
def on_connect(client, userdata, flags, rc, properties=None):
    print(f"✅ MQTT Conectado. Suscribiendo a {TOPIC_SUB}...")
    client.subscribe(TOPIC_SUB)

mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

while True:
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        mqtt_client.loop_forever()
    except Exception as e:
        print(f"💥 Error en loop MQTT: {e}. Reintentando...")
        time.sleep(5)