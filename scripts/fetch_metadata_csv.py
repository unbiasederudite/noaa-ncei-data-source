from noaa_ncei.api_data.metadata import Metadata 

print("Fetching Dataset metadata...")
metadata = Metadata()
dataset_metadata_file_path = metadata.fetch_datasets(format="csv")
print(f"Dataset metadata saved to {dataset_metadata_file_path}")

print("Fetching Data Category metadata for the 'GHCND' dataset...")
data_category_metadata_file_path = metadata.fetch_data_categories(format="csv", datasetid="GHCND")
print(f"Data Category metadata saved to {data_category_metadata_file_path}")

print("Fetching Data Type metadata for the 'GHCND' dataset...")
data_type_metadata_file_path = metadata.fetch_data_types(format="csv", datasetid="GHCND")
print(f"Data Type metadata saved to {data_type_metadata_file_path}")

print("Fetching Location Category metadata for the 'GHCND' dataset...")
location_category_metadata_file_path = metadata.fetch_location_categories(format="csv", datasetid="GHCND")
print(f"Location Category metadata saved to {location_category_metadata_file_path}")

print("Fetching Location metadata for the 'GHCND' dataset...")
location_metadata_file_path = metadata.fetch_locations(format="csv", datasetid="GHCND")
print(f"Location metadata saved to {location_metadata_file_path}")

print("Fetching Station metadata for the 'GHCND' dataset...")
station_metadata_file_path = metadata.fetch_stations(format="csv", datasetid="GHCND")
print(f"Station metadata saved to {station_metadata_file_path}")