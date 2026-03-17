import time
import sys
import paho.mqtt.client as mqtt
import psycopg2
import os
import json

# --- CONFIGURACIÓN DESDE RAILWAY ---

DB_URL = os.getenv("DATABASE_URL")

MQTT_BROKER = os.getenv("MQTT_HOST", "mqtt-server")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))

# --- 1. CONEXIÓN A POSTGRES CON REINTENTOS ---
conn = None
cursor = None

print("🚀 Iniciando Bridge MQTT a Postgres...")

while True:
    try:
        print(f"🔗 Intentando conectar a la DB en Railway...")
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS telemetria (
                id SERIAL PRIMARY KEY,
                topic TEXT,
                payload JSONB,
                fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
        print("✅ Base de Datos conectada y tabla lista.")
        break
    except Exception as e:
        print(f"⏳ Postgres no disponible, reintentando en 5s... ({e})")
        time.sleep(5)

# --- 2. LÓGICA DE RECEPCIÓN MQTT ---
def on_message(client, userdata, msg):
    try:
       
        raw_payload = msg.payload.decode()
        
        try:
            payload_to_store = json.loads(raw_payload)
        except:
            payload_to_store = json.dumps({"raw_text": raw_payload})

       
        query = "INSERT INTO telemetria (topic, payload) VALUES (%s, %s)"
        cursor.execute(query, (msg.topic, json.dumps(payload_to_store)))
        conn.commit()
        print(f"📥 [OK] Datos de {msg.topic} guardados en DB.")
        
    except Exception as e:
        print(f"❌ Error al procesar mensaje: {e}")
        if conn:
            conn.rollback()

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print(f"✅ Conectado al Broker MQTT interno ({MQTT_BROKER})")
        client.subscribe("universidad/jaen/#")
        print("📡 Suscrito a: universidad/jaen/#")
    else:
        print(f"❌ Error de conexión MQTT. Código: {rc}")

# --- 3. CONFIGURACIÓN MQTT CON REINTENTOS ---
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_connect = on_connect
client.on_message = on_message

while True:
    try:
        print(f"📡 Intentando conectar a MQTT en {MQTT_BROKER}...")
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        break
    except Exception as e:
        print(f"⏳ Broker no encontrado o DNS no listo, reintentando en 5s... ({e})")
        time.sleep(5)

# Bucle infinito de escucha
try:
    client.loop_forever()
except KeyboardInterrupt:
    print("🛑 Bridge detenido por el usuario.")
finally:
    if cursor: cursor.close()
    if conn: conn.close()