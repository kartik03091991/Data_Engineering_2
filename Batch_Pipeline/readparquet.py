import pyarrow.parquet as pq


def yellowtaxi_Oct2022():
    df = pq.read_table(source='D:/Aditya/SRH/Data Engineering 2/Dataset/yellow_tripdata_Oct2022.parquet').to_pandas()

    # determining the name of the file
    file_name = 'D:/Aditya/SRH/Data Engineering 2/Dataset/TaxidataYellow_Oct2022.csv'

    # saving the excel
    df.to_csv(file_name)
    print('DataFrame is written to Excel File successfully.')


def yellowtaxi_Nov2022():
    df = pq.read_table(source='D:/Aditya/SRH/Data Engineering 2/Dataset/yellow_tripdata_Nov2022.parquet').to_pandas()

    # determining the name of the file
    file_name = 'D:/Aditya/SRH/Data Engineering 2/Dataset/TaxidataYellow_Nov2022.csv'

    # saving the excel
    df.to_csv(file_name)
    print('DataFrame is written to Excel File successfully.')
    

def yellowtaxi_Dec2022():
    df = pq.read_table(source='D:/Aditya/SRH/Data Engineering 2/Dataset/yellow_tripdata_Dec2022.parquet').to_pandas()

    # determining the name of the file
    file_name = 'D:/Aditya/SRH/Data Engineering 2/Dataset/TaxidataYellow_Dec2022.csv'

    # saving the excel
    df.to_csv(file_name)
    print('DataFrame is written to Excel File successfully.')
        

def yellowtaxi_Jan2023():
    df = pq.read_table(source='D:/Aditya/SRH/Data Engineering 2/Dataset/yellow_tripdata_Jan2023.parquet').to_pandas()

    # determining the name of the file
    file_name = 'D:/Aditya/SRH/Data Engineering 2/Dataset/TaxidataYellow_Jan2023.csv'

    # saving the excel
    df.to_csv(file_name)
    print('DataFrame is written to Excel File successfully.')


def yellowtaxi_Feb2023():
    df = pq.read_table(source='D:/Aditya/SRH/Data Engineering 2/Dataset/yellow_tripdata_Feb2023.parquet').to_pandas()

    # determining the name of the file
    file_name = 'D:/Aditya/SRH/Data Engineering 2/Dataset/TaxidataYellow_Feb2023.csv'

    # saving the excel
    df.to_csv(file_name)
    print('DataFrame is written to Excel File successfully.')


def yellowtaxi_March2023():
    df = pq.read_table(source='D:/Aditya/SRH/Data Engineering 2/Dataset/yellow_tripdata_March2023.parquet').to_pandas()

    # determining the name of the file
    file_name = 'D:/Aditya/SRH/Data Engineering 2/Dataset/TaxidataYellow_March2023.csv'

    # saving the excel
    df.to_csv(file_name)
    print('DataFrame is written to Excel File successfully.')



#yellowtaxi_Oct2022()
yellowtaxi_Nov2022()
yellowtaxi_Dec2022()
yellowtaxi_Jan2023()
yellowtaxi_Feb2023()
yellowtaxi_March2023()
