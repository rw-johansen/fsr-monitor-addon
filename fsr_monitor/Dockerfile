ARG BUILD_FROM
FROM $BUILD_FROM

# Installer afhængigheder
COPY requirements.txt /tmp/
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# Kopiér scripts
COPY run.sh /
COPY fsr_monitor_mqtt.py /

RUN chmod +x /run.sh

CMD ["/run.sh"]
