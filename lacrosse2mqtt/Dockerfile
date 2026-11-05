ARG BUILD_FROM=ghcr.io/home-assistant/amd64-base:3.19
FROM $BUILD_FROM

# Nur stdlib — keine pip-Pakete noetig
RUN apk add --no-cache python3

WORKDIR /
COPY lacrosse2mqtt.py /
COPY run.sh /
RUN chmod a+x /run.sh

# s6-Overlay des Base-Images umgehen (suexec verlangt PID 1) — eigener Entrypoint
ENTRYPOINT [ "/bin/sh", "/run.sh" ]
CMD [ "/run.sh" ]

COPY healthcheck.py /app/healthcheck.py
HEALTHCHECK --interval=60s --timeout=10s --start-period=180s --retries=3 \
  CMD python3 /app/healthcheck.py
