from geopy.geocoders import Nominatim
import csv


file_path = 'D:/DE2/Project_1/FileReadPykafka/taxi_zone_lookup.csv'

def send_csv_rows():
    with open(file_path, 'r') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)  # Skip header row if needed

        for row in csv_reader:
            message = ','.join(row)
            data = message.split(',')
            # Create a geocoder instance for Nominatim
            geolocator = Nominatim(user_agent="my-app")
            print(data[2])

            if str(data[2]).find('/'):
                lst1 = (str(data[2]).replace('/',',')).split(',')
                print(lst1)
                for x in range(0,len(lst1)):
                    try:
                        geolocator = Nominatim(user_agent="my-app")
                        zone = str(lst1[x]) + ","+ str(data[1]) + ", NY, USA"
                        print(zone)
                        # Perform geocoding for a specific address
                        #location = geolocator.geocode("New York City, USA")
                        
                        loc = 'Elmhurst, Queens, NY, USA'

                        location = geolocator.geocode(loc)
                        location = geolocator.geocode(zone)

                        # Retrieve latitude and longitude coordinates
                        latitude = location.latitude
                        longitude = location.longitude

                        # Print the coordinates
                        #print(f"Latitude: {latitude}, Longitude: {longitude}")
                        with open('lat_long_loc_1.txt', 'a') as file:
                            # Write the parameters to the file
                            file.write(str(data[0]) + " " + "," + zone + " "+ ","+ data[3] + " "+ ","+ str(latitude)  + " " + ","+ str(longitude) + "\n")
                            # file.write(latitude+ "\n")
                            # file.write(longitude + "\n")
                            print(zone, latitude, longitude)
                    except Exception as e:
                        with open('lat_long_loc_1.txt', 'a') as file:
                            # Write the parameters to the file
                            file.write(str(data[0]) + " " +  zone + " " + 'Location not present' + "\n")
                            print('Location not present', zone)
            else:
                try:
                    zone = str(lst1[x]) + ","+ str(data[1]) + ", NY, USA"
                    #print(zone)
                    # Perform geocoding for a specific address
                    #location = geolocator.geocode("New York City, USA")
                    location = geolocator.geocode(zone)

                    # Retrieve latitude and longitude coordinates
                    latitude = location.latitude
                    longitude = location.longitude

                    # Print the coordinates
                    #print(f"Latitude: {latitude}, Longitude: {longitude}")
                    with open('lat_long_loc_1.txt', 'a') as file:
                            # Write the parameters to the file
                        file.write(str(data[0]) + " " + "," + zone + " "+ ","+ data[3] + " "+ ","+ str(latitude)  + " " + ","+ str(longitude) + "\n")
                        print(zone, latitude, longitude)
                except Exception as e:
                        with open('lat_long_loc_1.txt', 'a') as file:
                            # Write the parameters to the file
                            file.write(str(data[0]) + " " + zone + " " + 'Location not present' + "\n")
                            print('Location not present', zone)
                break
                
            
# Start sending CSV rows to Kafka
send_csv_rows()

# # Create a geocoder instance for Nominatim
# geolocator = Nominatim(user_agent="my-app")

# # Perform geocoding for a specific address
# #location = geolocator.geocode("New York City, USA")
# location = geolocator.geocode("Newark Airport, USA")

# # Retrieve latitude and longitude coordinates
# latitude = location.latitude
# longitude = location.longitude

# # Print the coordinates
# print(f"Latitude: {latitude}, Longitude: {longitude}")
