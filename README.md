# FireServiceRota Monitor – Home Assistant Add-on

Lytter på FireServiceRota WebSocket i realtid og sender udkalds- og bemandingsstatus til Home Assistant via MQTT.

## Features

- 🚨 **Udkaldsstatus** – per-person respons publiceres som MQTT-sensorer
- 🚒 **Bemanding** – tilgængelige personer og skill-dækning opdateres løbende  
- 👤 **Alle brugere oprettes ved opstart** – ingen ventetid på første udkald
- 🔍 **Auto-discovery** – sensorer oprettes automatisk i HA

## MQTT-emner

| Emne | Indhold |
|------|---------|
| `fsr/incident/state` | Aktiv hændelse |
| `fsr/incident/response/<user_id>` | Udkaldsstatus pr. person |
| `fsr/availability/<ar_id>` | Stationens bemandingsstatus |
| `fsr/user/<user_id>/availability` | Tilgængelighed pr. person |
| `fsr/status` | online / offline |

## Installation

1. **Indstillinger → Add-ons → Add-on butik → ⋮ → Repositories**
2. Tilføj URL til dette repo
3. Installér **FireServiceRota Monitor**
4. Udfyld email, password og group_id i konfiguration
5. Start add-on'en
