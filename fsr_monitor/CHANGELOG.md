# Changelog

## 1.2.1
- Tilføjet support for availability_code_id 662 ("Sidst ved udkald") og
  909 ("On Mission") – vises nu som status_label på samme måde som
  "Tilgængelig"/"Ikke tilgængelig" når koden er aktiv på en person


## 1.2.0
- **Stor ændring:** Availability hentes nu via REST-polling i stedet for
  WebSocket-deltas. Bekræftet endpoint:
  `GET /api/v2/memberships/combined_schedules?group_ids=X&start_time=nu`
- Løser rodproblemet bag "alle står som utilgængelig efter genstart" –
  WebSocket-kanalen sendte kun ændringer, ikke et fuldt snapshot ved
  (gen)abonnement
- Fjerner al klient-side tidszone-matching – da vi spørger om "nu" får vi
  altid det korrekte interval direkte fra serveren
- Bemandingskrav (minimum pr. skill) hentes separat via
  `GET /api/v2/availability_requirements?group_id=X` og kombineres med
  live-data for at vise korrekt over/under-bemanding
- "Genopfrisk tilgængelighed"-knappen og den automatiske opdatering hvert
  minut bruger nu begge det samme REST-endpoint – øjeblikkeligt resultat,
  ingen ventetid på WebSocket-beskeder


## 1.1.5
- Diagnose: WebSocket-kanalen for availability sender sandsynligvis kun
  deltas (ændringer), ikke et fuldt snapshot ved abonnement – dette
  forklarer hvorfor alle stod som "Ikke tilgængelig" efter genstart
- Tilføjet REST-baseret snapshot-forsøg ved opstart og ved manuel
  genopfriskning. Flere sandsynlige endpoints afprøves og logges
  med [SNAPSHOT]-præfiks, da det eksakte endpoint ikke er bekræftet endnu


## 1.1.4
- Ny knap i HA: "FSR – Genopfrisk tilgængelighed" (mqtt button-entitet)
  der øjeblikkeligt genberegner availability og gen-abonnerer på FSR
  for at hente et frisk snapshot
- Automatisk genberegning hvert minut i baggrunden, så et interval-skifte
  opdages selv uden en ny WebSocket-besked fra FSR


## 1.1.3
- Fix: tidszone-bug i availability-interval matching der fik personer til at
  fremstå utilgængelige for tidligt (fx ~1 time før rota-slut)
- Tilføjet midlertidig logning af valgt interval til fejlfinding


## 1.1.2
- Nyt FSR logo som ikon
- group_id er nu tomt som standard

## 1.1.1
- group_id standardværdi fjernet

## 1.1.0
- Alle brugere oprettes som sensorer ved opstart
- Tilgængeligheds-sensor pr. bruger (fsr/user/<id>/availability)
- MQTT paho 2.x kompatibilitet

## 1.0.0
- Første version
- Incident respons via MQTT
- Availability via MQTT
- Auto-discovery i Home Assistant
