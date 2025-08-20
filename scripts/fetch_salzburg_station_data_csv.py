from noaa_ncei.api_data.station_data import StationData

munich_data = StationData("GHCND:AU000006306")

print("Fetching Salzburg Station information...")
station_info_file_path = munich_data.fetch_station_info(format="csv")
print(f"Salzburg Station information saved to {station_info_file_path}")

print("Fetching Salzburg Station Datasets data...")
station_datasets_file_path = munich_data.fetch_station_datasets(format="csv")
print(f"Salzburg Station Datasets data saved to {station_datasets_file_path}")

print("Fetching Salzburg Station Categories Data data for the 'GHCND' dataset...")
station_data_categories_file_path = munich_data.fetch_station_data_categories(format="csv", datasetid="GHCND")
print(f"Salzburg Station Categories data saved to {station_data_categories_file_path}")

print("Fetching Salzburg Station Data Types data for the 'GHCND' dataset...")
station_data_types_file_path = munich_data.fetch_station_data_types(format="csv", datasetid="GHCND")
print(f"Salzburg Station Data Types data saved to {station_data_types_file_path}")

print("Fetching Salzburg Station Data from 2025-01-01 to 2025-02-01 for the 'GHCND' dataset...")
station_data_file_path = munich_data.fetch_station_data(format="csv", datasetid="GHCND", startdate="2025-01-01", enddate="2025-02-01")
print(f"Salzburg Station Data saved to {station_data_file_path}")
