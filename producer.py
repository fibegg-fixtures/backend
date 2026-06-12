import base64
import json
import os
import time
from io import BytesIO

import requests
from kafka import KafkaProducer
from PIL import Image

INTERVAL = int(os.getenv("X", 5))
TOPIC = os.getenv("TOPIC", "people")
KAFKA = os.getenv("KAFKA", "kafka:29092")


def person():
    print("fetching image", flush=True)
    res = requests.get(
        "https://thispersondoesnotexist.com/",
        headers={"User-Agent": "curl/8"},
        timeout=(5, 10),
    )
    res.raise_for_status()

    img = Image.open(BytesIO(res.content))
    img.thumbnail((420, 420))
    out = BytesIO()
    img.save(out, format="JPEG", quality=75)

    return {
        "content_type": "image/jpeg",
        "image_b64": base64.b64encode(out.getvalue()).decode(),
    }


def producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA,
        max_block_ms=5000,
        value_serializer=lambda value: json.dumps(value).encode(),
    )


while True:
    try:
        msg = person()
        sender = producer()
        print("sending", msg["content_type"], flush=True)
        sender.send(TOPIC, msg)
        sender.flush()
        sender.close()
        print("sent", len(msg["image_b64"]), "chars", flush=True)
        time.sleep(INTERVAL)
    except Exception as error:
        print("waiting", error, flush=True)
        time.sleep(2)
