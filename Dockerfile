FROM eclipse-mosquitto:latest

# Añade esta variable de entorno al Dockerfile
ENV PYTHONUNBUFFERED=1
# O asegúrate de que el comando de ejecución no bloquee los logs
CMD ["/usr/sbin/mosquitto", "-c", "/mosquitto/config/mosquitto.conf"]

# Copiamos una configuración básica
COPY mosquitto.conf /mosquitto/config/mosquitto.conf

# Exponemos el puerto estándar
EXPOSE 1883