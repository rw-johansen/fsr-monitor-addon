#!/usr/bin/with-contenv bashio

# Læs konfiguration fra HA add-on options
export FSR_EMAIL=$(bashio::config 'email')
export FSR_PASSWORD=$(bashio::config 'password')
export FSR_GROUP_ID=$(bashio::config 'group_id')
export FSR_MQTT_HOST=$(bashio::config 'mqtt_host')
export FSR_MQTT_PORT=$(bashio::config 'mqtt_port')
export FSR_MQTT_USER=$(bashio::config 'mqtt_user')
export FSR_MQTT_PASSWORD=$(bashio::config 'mqtt_password')
export FSR_MQTT_PREFIX=$(bashio::config 'mqtt_prefix')

bashio::log.info "Starter FireServiceRota Monitor..."
bashio::log.info "Group ID: ${FSR_GROUP_ID}"
bashio::log.info "MQTT: ${FSR_MQTT_HOST}:${FSR_MQTT_PORT}"

exec python3 /fsr_monitor_mqtt.py
