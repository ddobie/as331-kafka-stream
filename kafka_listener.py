import json
from confluent_kafka import Consumer
 
consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "vast-measurement-consumers",
    "auto.offset.reset": "earliest",
})
consumer.subscribe(["casda-as331-filtered.measurements"])
 
while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print("consumer error:", msg.error())
        continue
    source_id = msg.key().decode("utf-8")
    measurement = json.loads(msg.value().decode("utf-8"))
    print(source_id, measurement)
