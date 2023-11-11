import datetime
import json
import time
import random
from kafka import KafkaProducer

kafka_server = 'localhost:9092'
topic = 'sample-topic'
delay = 3

def json_serializer(data):
    return json.dumps(data)

producer = KafkaProducer(
    bootstrap_servers = [kafka_server],
    value_serializer = json_serializer
    )

#celcius
def getTempData():
    tempData = {
        "sensorId": random.randint(1, 5),
        "type": "TEMP",
        "value": random.randint(20, 35),
        "time": str(datetime.datetime.now())
    }

    return json_serializer(tempData)

#percent
def getHumidityData():
    tempData = {
        "sensorId": random.randint(1, 5),
        "type": "HUMI",
        "value": round(random.uniform(20, 65), 2),
        "time": str(datetime.datetime.now())
    }

    return json_serializer(tempData)

#pH
def getSoilpHData():
    tempData = {
        "sensorId": random.randint(1, 5),
        "type": "SOIL",
        "value": round(random.uniform(6.5, 7.5), 2),
        "time": str(datetime.datetime.now())
    }

    return json_serializer(tempData)

#lit/minute
def getWaterFlowData():
    tempData = {
        "sensorId": random.randint(1, 5),
        "type": "WATER",
        "value": round(random.uniform(0, 2), 2),
        "time": str(datetime.datetime.now())
    }

    return json_serializer(tempData)

while True:
    temp_data = getTempData()
    print("Send to topic: "  + topic + " -----  data: " + temp_data)
    producer.send(topic ,temp_data)

    humi_data = getHumidityData()
    print("Send to topic: " + topic + " -----  data: " + humi_data)
    producer.send(topic, humi_data)

    soil_ph_data = getSoilpHData()
    print("Send to topic: " + topic + " -----  data: " + soil_ph_data)
    producer.send(topic, soil_ph_data)

    water_flow_data = getWaterFlowData()
    print("Send to topic: " + topic + " -----  data: " + water_flow_data)
    producer.send(topic, water_flow_data)

    time.sleep(delay)
