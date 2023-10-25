from time import time
from kafka import KafkaConsumer
import json, time


topic = 'sample-topic'

def json_serializer(data):
    return json.dumps(data)

producer = KafkaProducer(
    bootstrap_servers = ['localhost:9092'],
    value_serializer = json_serializer
    )

while True:
    user = get_register()
    print(user)

    tempData = {
        "sensorId" : random.randint(1, 5),
        "temp": random.randint(35, 40)
    }

    json_data = json.dumps(data)

    producer.send(topic ,json_data)
    time.sleep(3)