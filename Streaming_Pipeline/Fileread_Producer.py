from time import sleep
from json import dumps
from kafka import KafkaProducer
import pandas as pd
from kafka  import KafkaConsumer
import pandas as pd
from sodapy import Socrata
import csv

topic_name='test1'
producer = KafkaProducer(bootstrap_servers=['localhost:9092']) # ,value_serializer=lambda x: dumps(x).encode('utf-8'))
client = Socrata(domain  = "data.cityofnewyork.us",
                 app_token = "wQDKWDENoNdTlOjc1BNEoKXul",
                 username="kartikeyasharma03@gmail.com",
                 password="newyorktaxi@123")

results = client.get("djnb-wcxt", limit=500)
results_df = pd.DataFrame.from_records(results)
#results_df.fillna(value=None, inplace=True)
print(results_df)
num_rows = results_df.shape[0]

for x in range(0,num_rows+1):
    row = results_df.iloc[x]
    row = row.to_list()
    #message = ','.join((row.to_list()))
    lst1 = [str(x) for x in row]
    #print((lst1))
    #print(' '.join(lst1))
    
    message = ','.join(lst1)

    try:
        # Send the message to Kafka
        producer.send(topic_name, message.encode('utf-8'))
        print('Sent message:', message)
        
    except Exception as e:
        print('Failed to send message:', str(e))
    sleep(1)
    #break
