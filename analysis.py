import time
import psycopg2
import datetime
from matplotlib import pyplot as plt
import pandas as pd

def query_for_analysis():
    try:
        conn = psycopg2.connect(database = "smart_farm", 
                        user = "root", 
                        host= 'localhost',
                        password = "root",
                        port = 5432)
        sql_query = "SELECT temperature, humidity, precipitation, observation_time FROM weather WHERE observation_time > now() + interval '7 hour'  - interval '20 minute' ORDER BY observation_time;"
        df = pd.read_sql_query(sql_query, conn)

        # Line chart for temperature
        df['observation_time'] = pd.to_datetime(df['observation_time'])
        df = df.set_index('observation_time')
        plt.title('Line chart of temperature in today')
        plt.xlabel('DateTime', fontsize=15)
        plt.ylabel('Temperature', fontsize=15)
        plt.plot(df.index, df['temperature'])
        plt.show()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

if __name__ == "__main__":
    query_for_analysis()