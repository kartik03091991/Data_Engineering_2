import pyarrow.parquet as pq


def greentaxi_Oct2022():
    df = pq.read_table(source='D:/Aditya/SRH/Data Engineering 2/Dataset/green_tripdata_Oct2022.parquet').to_pandas()

    # determining the name of the file
    file_name = 'D:/Aditya/SRH/Data Engineering 2/Dataset/Taxidatagreen_Oct2022.csv'

    # saving the excel
    df.to_csv(file_name)
    print('DataFrame is written to Excel File successfully.')


def greentaxi_Nov2022():
    df = pq.read_table(source='D:/Aditya/SRH/Data Engineering 2/Dataset/green_tripdata_Nov2022.parquet').to_pandas()

    # determining the name of the file
    file_name = 'D:/Aditya/SRH/Data Engineering 2/Dataset/Taxidatagreen_Nov2022.csv'

    # saving the excel
    df.to_csv(file_name)
    print('DataFrame is written to Excel File successfully.')
    

def greentaxi_Dec2022():
    df = pq.read_table(source='D:/Aditya/SRH/Data Engineering 2/Dataset/green_tripdata_Dec2022.parquet').to_pandas()

    # determining the name of the file
    file_name = 'D:/Aditya/SRH/Data Engineering 2/Dataset/Taxidatagreen_Dec2022.csv'

    # saving the excel
    df.to_csv(file_name)
    print('DataFrame is written to Excel File successfully.')
        

def greentaxi_Jan2023():
    df = pq.read_table(source='D:/Aditya/SRH/Data Engineering 2/Dataset/green_tripdata_Jan2023.parquet').to_pandas()

    # determining the name of the file
    file_name = 'D:/Aditya/SRH/Data Engineering 2/Dataset/Taxidatagreen_Jan2023.csv'

    # saving the excel
    df.to_csv(file_name)
    print('DataFrame is written to Excel File successfully.')


def greentaxi_Feb2023():
    df = pq.read_table(source='D:/Aditya/SRH/Data Engineering 2/Dataset/green_tripdata_Feb2023.parquet').to_pandas()

    # determining the name of the file
    file_name = 'D:/Aditya/SRH/Data Engineering 2/Dataset/Taxidatagreen_Feb2023.csv'

    # saving the excel
    df.to_csv(file_name)
    print('DataFrame is written to Excel File successfully.')


def greentaxi_March2023():
    df = pq.read_table(source='D:/Aditya/SRH/Data Engineering 2/Dataset/green_tripdata_March2023.parquet').to_pandas()

    # determining the name of the file
    file_name = 'D:/Aditya/SRH/Data Engineering 2/Dataset/Taxidatagreen_March2023.csv'

    # saving the excel
    df.to_csv(file_name)
    print('DataFrame is written to Excel File successfully.')



greentaxi_Oct2022()
greentaxi_Nov2022()
greentaxi_Dec2022()
greentaxi_Jan2023()
greentaxi_Feb2023()
greentaxi_March2023()
