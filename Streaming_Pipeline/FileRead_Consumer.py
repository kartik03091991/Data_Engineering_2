from kafka import KafkaProducer, KafkaConsumer
import mysql.connector

consumer = KafkaConsumer( 'test1', bootstrap_servers=['localhost:9092'])

# Consume data from Kafka
for message in consumer:
    # Convert byte object to string using UTF-8 encoding
    string_object = message.value.decode('utf-8')
    Data = string_object.split(',')
    print(Data)
    #print(Data[0])
    VendorID = None if Data[0] == 'nan' else Data[0]
    lpep_pickup_datetime = None if Data[1] == 'nan' else Data[1]
    lpep_dropoff_datetime = None if Data[2] == 'nan' else Data[2]
    store_and_fwd_flag = None if Data[3] == 'nan' else Data[3]
    RatecodeID = None if Data[4] == 'nan' else Data[4]
    PULocationID = None if Data[5] == 'nan' else Data[5]
    DOLocationID = None if Data[6] == 'nan' else Data[6]
    passenger_count = None if Data[7] == 'nan' else Data[7]
    trip_distance = None if Data[8] == 'nan' else Data[8]
    fare_amount = None if Data[9] == 'nan' else Data[9]
    extra = None if Data[10] == 'nan' else Data[10]
    mta_tax = None if Data[11] == 'nan' else Data[11]
    tip_amount = None if Data[12] == 'nan' else Data[12]
    tolls_amount = None if Data[13] == 'nan' else Data[13]
    improvement_surcharge = None if Data[14] == 'nan' else Data[14]
    total_amount = None if Data[15] == 'nan' else Data[15]
    payment_type = None if Data[16] == 'nan' else Data[16]
    trip_type = None if Data[17] == 'nan' else Data[17]
    congestion_surcharge = None if Data[18] == 'nan' else Data[18]




    print(VendorID,lpep_pickup_datetime,lpep_dropoff_datetime,store_and_fwd_flag,RatecodeID,PULocationID,DOLocationID,passenger_count,trip_distance,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,payment_type,trip_type,congestion_surcharge) 
#consumer.close()

    # Establish a connection to the MySQL database
    cnx = mysql.connector.connect(
        host="localhost",
        user="root",
        password="mysql123",
        database="taxi"
    )


    # Create a cursor object to interact with the database
    cursor = cnx.cursor()



    # Execute a simple query
    #query = "SELECT * FROM carrental.watermeter LIMIT 10"
    #query = "INSERT INTO TAXI.GreenTaxi (VendorID,lpep_pickup_datetime,lpep_dropoff_datetime,store_and_fwd_flag,RatecodeID,PULocationID,DOLocationID,passenger_count,trip_distance,fare_amount,extra,mta_tax,tip_amount,tolls_amount,ehail_fee,improvement_surcharge,total_amount,payment_type,trip_type,congestion_surcharge) VALUES (VendorID,lpep_pickup_datetime,lpep_dropoff_datetime,store_and_fwd_flag,RatecodeID,PULocationID,DOLocationID,passenger_count,trip_distance,fare_amount,extra,mta_tax,tip_amount,tolls_amount,ehail_fee,improvement_surcharge,total_amount,payment_type,trip_type,congestion_surcharge)"
    try:
        # Send the message to Kafka
        query = "INSERT INTO TAXI.GreenTaxi  (VendorID,lpep_pickup_datetime,lpep_dropoff_datetime,store_and_fwd_flag,RatecodeID,PULocationID,DOLocationID,passenger_count,trip_distance,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,payment_type,trip_type,congestion_surcharge) VALUES ( %s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s)"

        # Data to be inserted
        data = (VendorID,lpep_pickup_datetime,lpep_dropoff_datetime,store_and_fwd_flag,RatecodeID,PULocationID,DOLocationID,passenger_count,trip_distance,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,payment_type,trip_type,congestion_surcharge)
        
        cursor.execute(query,data)
        
    except Exception as e:
        print('Failed to load this row:', str(e))
    # query = "INSERT INTO TAXI.GreenTaxi  (VendorID,lpep_pickup_datetime,lpep_dropoff_datetime,store_and_fwd_flag,RatecodeID,PULocationID,DOLocationID,passenger_count,trip_distance,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,payment_type,trip_type,congestion_surcharge) VALUES ( %s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s)"

    # # Data to be inserted
    # data = (VendorID,lpep_pickup_datetime,lpep_dropoff_datetime,store_and_fwd_flag,RatecodeID,PULocationID,DOLocationID,passenger_count,trip_distance,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,payment_type,trip_type,congestion_surcharge)
    
    # cursor.execute(query,data)

    # Fetch all the results
    #results = cursor.fetchall()

    # Process the results
    #for row in results:
    #    print(row)

    # Close the cursor and connection
    cnx.commit()
    cursor.close()
    cnx.close()