from confluent_kafka import Consumer
import os, json, time

c = Consumer({
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP','kafka:9092'),
    'group.id': 'cli-consumer',
    'auto.offset.reset': 'earliest'
})
c.subscribe([os.getenv('KAFKA_TOPIC','market.ticks')])
print("Starting consumer...")
cnt = 0
start = time.time()
while True:
    msg = c.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print("Error:", msg.error())
        continue
    data = msg.value().decode('utf-8')
    cnt += 1
    if cnt % 1000 == 0:
        print(f"Consumed {cnt} messages. Last: {data[:200]}")
