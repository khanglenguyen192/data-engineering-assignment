from kafka import KafkaConsumer
import json
import os
import psycopg2

# Config postgreSQL
conn = psycopg2.connect(database = "smart_farm", 
                        user = "root", 
                        host= 'localhost',
                        password = "root",
                        port = 5432)

# Define topics for listening
topic_name_list = ["soil_topic", 'water_topic', 'weather_topic']

# Config kafka consumer
consumer = KafkaConsumer (
    bootstrap_servers = 'localhost:29092'
    # , group_id='sensors'
    , auto_offset_reset = 'latest'
    ,enable_auto_commit=True
    ,security_protocol =  'PLAINTEXT',
    #sasl_mechanism = 'SCRAM-SHA-512',
    #sasl_plain_username='admin',
    #sasl_plain_password='admin'
    )

def checking_weather_alert(temperature, humidity, precipitation, observation_time):
    alert = None
    if temperature > 40 or temperature < 10 or humidity < 0.3 or humidity > 0.6 or precipitation > 10:
        alert =  "Weather is harmful with temperature: {}, humidity: {}, precipitation: {} at {}.".format(temperature, humidity, precipitation, observation_time)
    return alert

def checking_moisture_lv_alert(sensor_id, moisture_lv, observation_time):
    alert = None
    if moisture_lv > 0.8 or moisture_lv < 0.2:
        alert =  "Soil is harmful measured by sensor: {}, with moisture level: {} at {} .".format(sensor_id, moisture_lv, observation_time)
    return alert

def checking_auto_decision():
    return

def consume_msg(topic_names):
    consumer.subscribe(topic_names)

    for message in consumer:
        try:
            msg = json.loads(message.value.decode("utf-8"))
            if msg != None:
                # sensor devices
                if 'sensor_id' in msg:
                    sensor_id, sensor_type, observation_time, value = msg['sensor_id'], msg['type'], msg['observation_time'], msg['value']
                    if sensor_type == 'SOIL':
                        soil_alert = checking_moisture_lv_alert(sensor_id, value, observation_time)
                        if soil_alert is not None:
                            cur = conn.cursor()
                            cur.execute("INSERT INTO alert_history(sensor_id, observation_time, alert_msg) VALUES({}, '{}', '{}')".format(sensor_id, observation_time, soil_alert))
                            conn.commit()

                            print('ALERT! ', soil_alert)

                            cur.close()
                # weather station
                else:
                    temperature, humidity, precipitation, observation_time = msg['temperature'], msg['humidity'], msg['precipitation'], msg['observation_time']
                    weather_alert = checking_weather_alert(temperature, humidity, precipitation, observation_time)
                    
                    # trigger alert for weather device
                    if weather_alert is not None:
                        cur = conn.cursor()
                        cur.execute("INSERT INTO alert_history(observation_time, alert_msg) VALUES('{}', '{}')".format(observation_time, weather_alert))
                        conn.commit()
                        print('ALERT! ', weather_alert)
                        cur.close()
        except:
            # print('exception! with: ', msg)
            # make a new transaction
            conn = psycopg2.connect(database = "smart_farm", 
                        user = "root", 
                        host= 'localhost',
                        password = "root",
                        port = 5432)
    conn.close()

# main function
if __name__== "__main__":
    consume_msg(topic_name_list)