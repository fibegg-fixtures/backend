import json
import os
import time
import uuid
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import Response, StreamingResponse
from kafka import KafkaConsumer

app = FastAPI()

KAFKA = os.getenv("KAFKA", "localhost:9092")
TOPIC = os.getenv("TOPIC", "people")

@app.middleware("http")
async def open_headers(request, call_next):
    response = Response("OK") if request.method == "OPTIONS" else await call_next(request)
    for name, value in {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "*",
        "Access-Control-Allow-Headers": "*",
        "Access-Control-Expose-Headers": "*",
        "Access-Control-Allow-Private-Network": "true",
    }.items():
        response.headers[name] = value
    return response


def branch():
    try:
        head = (Path(".git") / "HEAD").read_text().strip()
        return head.removeprefix("ref: refs/heads/") if head.startswith("ref:") else head[:7]
    except OSError:
        pass
    return os.getenv("GIT_BRANCH")


def stream():
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA,
                group_id=f"fastapi-sse-{uuid.uuid4()}",
                auto_offset_reset="latest",
                enable_auto_commit=True,
                bootstrap_timeout_ms=2000,
                value_deserializer=lambda data: json.loads(data.decode()),
            )
            yield 'event: status\ndata: {"ok": true}\n\n'
            try:
                for msg in consumer:
                    yield f"data: {json.dumps(msg.value)}\n\n"
            finally:
                consumer.close()
        except Exception as error:
            data = json.dumps({"ok": False, "error": str(error)})
            yield f"event: status\ndata: {data}\n\n"
            time.sleep(2)


@app.get("/events")
def events():
    return StreamingResponse(stream(), media_type="text/event-stream")


@app.get("/health")
def health():
    return {"ok": True, "branch": branch(), "topic": TOPIC}


@app.get("/")
def index():
    return {
        "ok": True,
        "branch": branch(),
        "sse": "/events",
        "example": "new EventSource('/events').onmessage = event => console.log(JSON.parse(event.data))",
    }
