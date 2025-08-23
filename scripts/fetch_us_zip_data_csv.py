from noaa_ncei.api_data.location_data import LocationData

us_zip_data = LocationData("ZIP:28801")

print("Fetching US ZIP Location Datasets data...")
location_datasets_file_path = us_zip_data.fetch_location_datasets(format="csv")
print(f"US ZIP Location Datasets data saved to {location_datasets_file_path}")

print("Fetching US ZIP Location Categories Data data for the 'GHCND' dataset...")
location_data_categories_file_path = us_zip_data.fetch_location_data_categories(format="csv", datasetid="GHCND")
print(f"US ZIP Location Categories data saved to {location_data_categories_file_path}")

print("Fetching US ZIP Location Data Types data for the 'GHCND' dataset...")
location_data_types_file_path = us_zip_data.fetch_location_data_types(format="csv", datasetid="GHCND")
print(f"US ZIP Location Data Types data saved to {location_data_types_file_path}")

print("Fetching US ZIP Location Stations data for the 'GHCND' dataset...")
location_stations_file_path = us_zip_data.fetch_location_stations(format="csv", datasetid="GHCND")
print(f"US ZIP Location Stations data saved to {location_stations_file_path}")

print("Fetching US ZIP Location Data from 2010-05-01 to 2010-05-01 for the 'GHCND' dataset...")
location_data_file_path = us_zip_data.fetch_location_data(format="csv", datasetid="GHCND", startdate="2010-05-01", enddate="2010-05-01")
print(f"US ZIP Location Data saved to {location_data_file_path}")