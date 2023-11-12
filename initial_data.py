
import json
import os
import psycopg2
import datetime

# field, sensor
num_of_field = 5
num_of_sensor = 10

conn = psycopg2.connect(database = "smart_farm", 
                        user = "root", 
                        host= 'localhost',
                        password = "root",
                        port = 5432)

for i in range(num_of_field):
    cur = conn.cursor()
    cur.execute("INSERT INTO field(field_id, field_name, area, location) VALUES({}, '{}', {}, '{}');".format(i, 'Number0'+str(i), i*2.5+1.5, '(10, 20{})'.format(i)))

    conn.commit()
    cur.close()

for j in range(num_of_sensor):
    cur = conn.cursor()
    if j < num_of_sensor/2:
        cur.execute("INSERT INTO sensor(sensor_id, field_id, sensor_type, installation_date) VALUES({}, '{}', '{}', '{}');".format(j, j%num_of_field, 'SOIL', datetime.date.today()))

        conn.commit()
    else:
        cur.execute("INSERT INTO sensor(sensor_id, field_id, sensor_type, installation_date) VALUES({}, '{}', '{}', '{}');".format(j, j%num_of_field, 'WATER', datetime.date.today()))

        conn.commit()
    cur.close()

conn.close()