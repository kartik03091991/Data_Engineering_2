
import mysql.connector
import pandas as pd

import csv
import time

#file_path = 'D:/DE2/Project_1/FileReadPykafka/green_tripdata_2023-03.csv'


#datetime_obj = datetime.datetime.strptime(datetime_str, "%m/%d/%Y %H:%M")

df = pd.read_csv('C:/Users/kulfi/OneDrive/SRH/Data Engineering 2/taxi_zone_lookup.csv')



# Establish a connection to the MySQL database
cnx = mysql.connector.connect(
    host="localhost",
    user="root",
    password="mysql123",
    database="taxi"
)

# Create a cursor object to interact with the database
#cursor1 = cnx.cursor()
cursor = cnx.cursor()


#deleting from the table first
sql = "DELETE FROM TAXI.taxi_zone_lookup"
cursor.execute(sql)
cnx.commit()


# loading the lookup table
for index, row in df.iterrows():
    sql = "INSERT INTO TAXI.taxi_zone_lookup (LocationID, Borough, Zone, service_zone) VALUES (%s, %s, %s, %s)"
    values = (row['LocationID'], row['Borough'], row['Zone'], row['service_zone'])
    cursor.execute(sql, values)
    cnx.commit()



cursor.close()
cnx.close()