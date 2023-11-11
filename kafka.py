import datetime
import json
import time
import random
from kafka import KafkaProducer

kafka_server = 'localhost:9092'
topic = 'sample-topic'
delay = 3

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

producer = KafkaProducer(
    bootstrap_servers = [kafka_server],
    value_serializer = json_serializer
    )

#celcius
def getTempData():
    data = {
        "sensorId": random.randint(1, 5),
        "type": "TEMP",
        "value": random.randint(20, 35),
        "time": str(datetime.datetime.now())
    }

    return data

#percent
def getHumidityData():
    data = {
        "sensorId": random.randint(1, 5),
        "type": "HUMI",
        "value": round(random.uniform(20, 65), 2),
        "time": str(datetime.datetime.now())
    }

    return data

#pH
def getSoilpHData():
    data = {
        "sensorId": random.randint(1, 5),
        "type": "SOIL",
        "value": round(random.uniform(6.5, 7.5), 2),
        "time": str(datetime.datetime.now())
    }

    return data

#lit/minute
def getWaterFlowData():
    data = {
        "sensorId": random.randint(1, 5),
        "type": "WATER",
        "value": round(random.uniform(0, 2), 2),
        "time": str(datetime.datetime.now())
    }

    return data

while True:
    temp_data = getTempData()
    print("Send to topic: "  + topic + " -----  data: " + str(temp_data))
    producer.send(topic, temp_data)

    humi_data = getHumidityData()
    print("Send to topic: " + topic + " -----  data: " + str(humi_data))
    producer.send(topic, humi_data)

    soil_ph_data = getSoilpHData()
    print("Send to topic: " + topic + " -----  data: " + str(soil_ph_data))
    producer.send(topic, soil_ph_data)

    water_flow_data = getWaterFlowData()
    print("Send to topic: " + topic + " -----  data: " + str(water_flow_data))
    producer.send(topic, water_flow_data)

    time.sleep(delay)
