FROM eclipse-mosquitto:latest

# Copiamos una configuración básica
COPY mosquitto.conf /mosquitto/config/mosquitto.conf

# Exponemos el puerto estándar
EXPOSE 1883