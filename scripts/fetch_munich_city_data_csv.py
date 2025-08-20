from noaa_ncei.api_data.location_data import LocationData

munich_data = LocationData("CITY:GM000019")

print("Fetching Munich Location information...")
location_info_file_path = munich_data.fetch_location_info(format="csv")
print(f"Munich Location information saved to {location_info_file_path}")

print("Fetching Munich Location Datasets data...")
location_datasets_file_path = munich_data.fetch_location_datasets(format="csv")
print(f"Munich Location Datasets data saved to {location_datasets_file_path}")

print("Fetching Munich Location Categories Data data for the 'GHCND' dataset...")
location_data_categories_file_path = munich_data.fetch_location_data_categories(format="csv", datasetid="GHCND")
print(f"Munich Location Categories data saved to {location_data_categories_file_path}")

print("Fetching Munich Location Data Types data for the 'GHCND' dataset...")
location_data_types_file_path = munich_data.fetch_location_data_types(format="csv", datasetid="GHCND")
print(f"Munich Location Data Types data saved to {location_data_types_file_path}")

print("Fetching Munich Location Stations data for the 'GHCND' dataset...")
location_stations_file_path = munich_data.fetch_location_stations(format="csv", datasetid="GHCND")
print(f"Munich Location Stations data saved to {location_stations_file_path}")

print("Fetching Munich Location Data from 2025-01-01 to 2025-02-01 for the 'GHCND' dataset...")
location_data_file_path = munich_data.fetch_location_data(format="csv", datasetid="GHCND", startdate="2025-01-01", enddate="2025-02-01")
print(f"Munich Location Data saved to {location_data_file_path}")
