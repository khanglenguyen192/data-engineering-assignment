import time
import psycopg2
import datetime


def monitoring():
    conn = None
    soil_lst = []
    water_lst = []
    soil_id_lst = []
    try:
        conn = psycopg2.connect(database = "smart_farm", 
                        user = "root", 
                        host= 'localhost',
                        password = "root",
                        port = 5432)
        cur = conn.cursor()
        cur.execute("SELECT sensor_id, type, observation_time, value FROM sensor_data WHERE observation_time > now() + interval '7 hour'  - interval '60 second' ORDER BY observation_time;")
        print("The number of parts: ", cur.rowcount)
        row = cur.fetchone()

        while row is not None:
            if row[1] == 'SOIL' and row[3] > 0.65:
                soil_lst.append(row)
            elif row[1] == 'WATER':
                water_lst.append(row)
            row = cur.fetchone()
        cur.close()
        
        cur = conn.cursor()
        for soil in soil_lst:
            soil_sensor_id = soil[0]
            if soil_sensor_id in soil_id_lst:
                continue
            soil_id_lst.append(soil_sensor_id)
            for water in water_lst:
                # checking 2 sensor (water and soil) is same field and watering system is running?
                if water[0] % 5 == soil_sensor_id and water[3] > 0:
                    now = datetime.datetime.now()
                    decision_msg = 'Auto Decision: Stop watering in field_id {} at {}'.format(soil_sensor_id, now)
                    print(decision_msg, '.Because of: soil_sensor_id {} - {}, water_sensor_id {} - {}'.format(soil_sensor_id, soil[3], water[0], water[3]))
                    cur.execute("INSERT INTO decision_history(sensor_id, observation_time, decision_msg) VALUES({}, '{}', '{}')".format(soil_sensor_id, now, decision_msg))
                    conn.commit()
                    break
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

if __name__== "__main__":
    delay = 10
    while True:
        monitoring()
        time.sleep(delay)