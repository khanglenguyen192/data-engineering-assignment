import datetime
import json
import time
import random
from kafka import KafkaProducer

# List of configurations
bootstrap_servers = 'localhost:29092'
security_protocol =  'PLAINTEXT'
sasl_mechanism =  'SCRAM-SHA-512'
weather_topic = 'weather_topic'
soil_topic = 'soil_topic'
water_topic = 'water_topic'
delay = 5

###############################################
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
security_protocol = security_protocol,
# sasl_mechanism = sasl_mechanism,
# sasl_plain_username = sasl_plain_username,
# sasl_plain_password = sasl_plain_password,
value_serializer=lambda x: json.dumps(x).encode('utf-8'),
key_serializer=lambda x: json.dumps(x).encode('utf-8')
)

#celcius
def getWeatherData():
    data = {
        "temperature": random.randint(20, 42),
        "humidity": round(random.uniform(0.3, 0.65), 2),
        "precipitation": random.randint(0, 11),
        "observation_time": str(datetime.datetime.now())
    }
    return data

# moisture level
def getMoistureLvData():
    data = {
        "sensor_id": random.randint(0, 4),
        "type": "SOIL",
        "value": round(random.uniform(0, 1), 2),
        "observation_time": str(datetime.datetime.now())
    }

    return data

# lit/minute
def getWaterFlowData():
    data = {
        "sensor_id": random.randint(5, 9),
        "type": "WATER",
        "value": round(random.uniform(0, 1), 2),
        "observation_time": str(datetime.datetime.now())
    }
    return data

while True:
    temp_data = getWeatherData()
    print("Send to topic: "  + weather_topic + " -----  data: " + str(temp_data))
    producer.send(weather_topic, temp_data)

    moisture_lv_data = getMoistureLvData()
    print("Send to topic: " + soil_topic + " -----  data: " + str(moisture_lv_data))
    producer.send(soil_topic, moisture_lv_data)

    water_flow_data = getWaterFlowData()
    print("Send to topic: " + water_topic + " -----  data: " + str(water_flow_data))
    producer.send(water_topic, water_flow_data)

    time.sleep(delay)