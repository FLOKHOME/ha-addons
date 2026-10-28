ARG BUILD_FROM=ghcr.io/home-assistant/amd64-base:3.19
FROM $BUILD_FROM

# Nur stdlib — keine pip-Pakete noetig
RUN apk add --no-cache python3

WORKDIR /
COPY lacrosse2mqtt.py /
COPY run.sh /
RUN chmod a+x /run.sh

CMD [ "/run.sh" ]
