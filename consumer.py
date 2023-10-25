from kafka import KafkaConsumer
import json
import os
import psycopg2

conn = psycopg2.connect(database = "smart_farm", 
                        user = "admin", 
                        host= 'localhost',
                        password = "admin",
                        port = 5432)

# Define topics for listening
topic_name_list = ["sensors"]

# Config kafka consumer
consumer = KafkaConsumer (
    bootstrap_servers = 'localhost'
    , group_id= 'sensors'
    , auto_offset_reset = 'latest'
    #enable_auto_commit=True
    #security_protocol =  'SASL_PLAINTEXT',
    #sasl_mechanism = 'SCRAM-SHA-512',
    #sasl_plain_username='admin',
    #sasl_plain_password='admin'
    )

def process_msg(msg):
    pass

# Hàm lấy thông tin từ topic (hứng topic từ K8s) và lưu vào file
def listening(topic_names):
    consumer.subscribe(topic_names)

    for message in consumer:
        try:
            msg = json.loads(message.value.decode("utf-8"))
            #print(msg['log'])
            # xét điều kiện log từ API với dạng LOG-REQ-RESP
            if "LOG-REQ-RESP" in msg['log']:
                result, table = process_msg(msg) # msg["log"] là kiểu str, xử lý log
                if result != None:
                    cur = conn.cursor()
                    cur.execute("INSERT INTO sensors(sensorID, value, when) VALUES('#012', 9, '25/10/2023')");

                    conn.commit()
                    cur.close()
        except:
            print('exception!')
    conn.close()

# # Chạy hàm main
if __name__== "__main__":
    listening(topic_name_list)