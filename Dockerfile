FROM eclipse-mosquitto:latest


ENV PYTHONUNBUFFERED=1

CMD ["/usr/sbin/mosquitto", "-c", "/mosquitto/config/mosquitto.conf"]


COPY mosquitto.conf /mosquitto/config/mosquitto.conf


EXPOSE 1883