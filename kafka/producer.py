import json
from confluent_kafka import Producer
import os
import requests
import time

API_vehicle = "https://api.irail.be/vehicle/"
API_liveboard = "https://api.irail.be/liveboard/"
TOPIC = "irail"
KAFKA = os.environ.get("KAFKA", "localhost:9092")
STATIONS = ["Namur"]
TRAINS = set()
INTERVAL = 60
producer = Producer({"bootstrap.servers": KAFKA})


def get_id_train(station):
    reponse = requests.get(
        API_liveboard, params={"station": station, "format": "json"}, timeout=10
    )
    reponse.raise_for_status()
    data = reponse.json()
    ids = []
    for starting in data["departures"]["departure"]:
        ids.append(starting["vehicle"])
    return ids


def get_train(train):
    reponse = requests.get(
        API_vehicle, params={"id": train, "format": "json"}, timeout=10
    )
    reponse.raise_for_status()
    return reponse.json()


if TOPIC not in producer.list_topics(timeout=10).topics:
    raise SystemExit(
        f"Le topic '{TOPIC}' n'existe pas : \
        creez-le d'abord (voir l'en-tete du fichier)."
    )

print("Interrogation Irail... (Ctrl+C to stop)")

next_call = {}
next_discovery = 0
try:
    while True:
        if time.monotonic() >= next_discovery:
            for station in STATIONS:
                try:
                    ids = get_id_train(station)
                    TRAINS.update(ids)
                except requests.RequestException as error:
                    print(f"Error for the station {station}: {error}")
                time.sleep(1)
        for train in TRAINS:
            now = time.monotonic()
            if now < next_call.get(train, 0):
                continue
            try:
                data = get_train(train)

                producer.produce(TOPIC, key=train, value=json.dumps(data))
                print(f"Données récupérées et mises en file Kafka : {train}")
                producer.poll(0)

            except requests.RequestException as error:
                print(f"Error for the train {train}: {error}")
            finally:
                next_call[train] = time.monotonic() + INTERVAL

            time.sleep(1)

        producer.poll(0.1)
except KeyboardInterrupt:
    pass
finally:
    producer.flush()
