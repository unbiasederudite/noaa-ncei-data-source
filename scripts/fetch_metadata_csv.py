from noaa_ncei.api_metadata.data_category_metadata import DataCategoryMetadata
from noaa_ncei.api_metadata.data_type_metadata import DataTypeMetadata
from noaa_ncei.api_metadata.dataset_metadata import DatasetMetadata
from noaa_ncei.api_metadata.location_category_metadata import LocationCategoryMetadata
from noaa_ncei.api_metadata.location_metadata import LocationMetadata
from noaa_ncei.api_metadata.station_metadata import StationMetadata

print("Fetching Dataset metadata...")
dataset_metadata = DatasetMetadata()
dataset_metadata_file_path = dataset_metadata.fetch_datasets(format="csv")
print(f"Dataset metadata saved to {dataset_metadata_file_path}")

print("Fetching Data Category metadata for the 'GHCND' dataset...")
data_category_metadata = DataCategoryMetadata()
data_category_metadata_file_path = data_category_metadata.fetch_data_categories(format="csv", datasetid="GHCND")
print(f"Data Category metadata saved to {data_category_metadata_file_path}")

print("Fetching Data Type metadata for the 'GHCND' dataset...")
data_type_metadata = DataTypeMetadata()
data_type_metadata_file_path = data_type_metadata.fetch_data_types(format="csv", datasetid="GHCND")
print(f"Data Type metadata saved to {data_type_metadata_file_path}")

print("Fetching Location Category metadata for the 'GHCND' dataset...")
location_category_metadata = LocationCategoryMetadata()
location_category_metadata_file_path = location_category_metadata.fetch_location_categories(format="csv", datasetid="GHCND")
print(f"Location Category metadata saved to {location_category_metadata_file_path}")

print("Fetching Location metadata for the 'GHCND' dataset...")
location_metadata = LocationMetadata()
location_metadata_file_path = location_metadata.fetch_locations(format="csv", datasetid="GHCND")
print(f"Location metadata saved to {location_metadata_file_path}")

print("Fetching Station metadata for the 'GHCND' dataset...")
station_metadata = StationMetadata()
station_metadata_file_path = station_metadata.fetch_stations(format="csv", datasetid="GHCND")
print(f"Station metadata saved to {station_metadata_file_path}")