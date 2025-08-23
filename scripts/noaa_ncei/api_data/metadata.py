from noaa_ncei.api_data.base_api_data import ApiData
from noaa_ncei.api_client.location_client import LocationClient
from noaa_ncei.api_client.location_category_client import LocationCategoryClient
from noaa_ncei.api_client.station_client import StationClient
from noaa_ncei.api_client.dataset_client import DatasetClient
from noaa_ncei.api_client.data_type_client import DataTypeClient
from noaa_ncei.api_client.data_category_client import DataCategoryClient
from noaa_ncei.api_data.config import API_TOKEN, CURRENT_FILE
import os

class Metadata(ApiData):
    def __init__(
            self,
            metadata_folder = os.path.join(CURRENT_FILE.parents[3], "metadata"),
            **kwargs 
    ):
        super().__init__(**kwargs)
        self.params={}

        self.metadata_folder = metadata_folder

        token = self.api_token or API_TOKEN

        self.location_client = LocationClient(token)
        self.location_category_client = LocationCategoryClient(token)
        self.station_client = StationClient(token)
        self.dataset_client = DatasetClient(token)
        self.data_category_client = DataCategoryClient(token)
        self.data_type_client = DataTypeClient(token)

    def fetch_data_categories(
        self,
        datasetid=None,
        locationid=None,
        stationid=None,
        startdate=None,
        enddate=None,
        format=None
    ):
        params_extra = {
            "startdate": startdate,
            "enddate": enddate,
            "datasetid": datasetid,
            "locationid": locationid,
            "stationid": stationid,
        }
        filename_parts = [
            f"start-{startdate}" if startdate else None,
            f"end-{enddate}" if enddate else None,
            f"dataset-{datasetid}" if datasetid else None,
            f"location-{locationid}" if locationid else None,
            f"station-{stationid}" if stationid else None,
        ]
        return self._fetch_generic(
            base_name="data_categories",
            params_extra=params_extra,
            csv_func=self.data_category_client.get_data_categories_csv,
            df_func=self.data_category_client.get_data_categories_df,
            default_func=self.data_category_client.get_data_categories,
            filename_parts=filename_parts,
            format=format,
            data_folder=self.metadata_folder
        )
    
    def fetch_data_types(
        self,
        datasetid=None,
        locationid=None,
        stationid=None,
        datacategoryid=None,
        startdate=None,
        enddate=None,
        format=None
    ):
        params_extra = {
            "startdate": startdate,
            "enddate": enddate,
            "datasetid": datasetid,
            "locationid": locationid,
            "stationid": stationid,
            "datacategoryid": datacategoryid,
        }
        filename_parts = [
            f"start-{startdate}" if startdate else None,
            f"end-{enddate}" if enddate else None,
            f"dataset-{datasetid}" if datasetid else None,
            f"location-{locationid}" if locationid else None,
            f"station-{stationid}" if stationid else None,
            f"datacategory-{datacategoryid}" if datacategoryid else None,
        ]
        return self._fetch_generic(
            base_name="data_types",
            params_extra=params_extra,
            csv_func=self.data_type_client.get_data_types_csv,
            df_func=self.data_type_client.get_data_types_df,
            default_func=self.data_type_client.get_data_types,
            filename_parts=filename_parts,
            format=format,
            data_folder=self.metadata_folder
        )
    
    def fetch_datasets(
        self,
        datatypeid=None,
        locationid=None,
        stationid=None,
        startdate=None,
        enddate=None,
        format=None
    ):
        params_extra = {
            "startdate": startdate,
            "enddate": enddate,
            "datatypeid": datatypeid,
            "locationid": locationid,
            "stationid": stationid,
        }
        filename_parts = [
            f"start-{startdate}" if startdate else None,
            f"end-{enddate}" if enddate else None,
            f"datatyp-{datatypeid}" if datatypeid else None,
            f"location-{locationid}" if locationid else None,
            f"station-{stationid}" if stationid else None,
        ]
        return self._fetch_generic(
            base_name="datasets",
            params_extra=params_extra,
            csv_func=self.dataset_client.get_datasets_csv,
            df_func=self.dataset_client.get_datasets_df,
            default_func=self.dataset_client.get_datasets,
            filename_parts=filename_parts,
            format=format,
            data_folder=self.metadata_folder
        )
    
    def fetch_location_categories(
        self,
        datasetid=None,
        startdate=None,
        enddate=None,
        format=None
    ):
        params_extra = {
            "startdate": startdate,
            "enddate": enddate,
            "datasetid": datasetid,
        }
        filename_parts = [
            f"start-{startdate}" if startdate else None,
            f"end-{enddate}" if enddate else None,
            f"dataset-{datasetid}" if datasetid else None,
        ]
        return self._fetch_generic(
            base_name="location_categories",
            params_extra=params_extra,
            csv_func=self.location_category_client.get_location_categories_csv,
            df_func=self.location_category_client.get_location_categories_df,
            default_func=self.location_category_client.get_location_categories,
            filename_parts=filename_parts,
            format=format,
            data_folder=self.metadata_folder
        )
    
    def fetch_locations(
        self,
        datasetid=None,
        locationcategoryid=None,
        startdate=None,
        enddate=None,
        format=None
    ):
        params_extra = {
            "startdate": startdate,
            "enddate": enddate,
            "datasetid": datasetid,
            "locationcategoryid": locationcategoryid,
        }
        filename_parts = [
            f"start-{startdate}" if startdate else None,
            f"end-{enddate}" if enddate else None,
            f"dataset-{datasetid}" if datasetid else None,
            f"locationcategory-{locationcategoryid}" if locationcategoryid else None
        ]
        return self._fetch_generic(
            base_name="locations",
            params_extra=params_extra,
            csv_func=self.location_client.get_locations_csv,
            df_func=self.location_client.get_locations_df,
            default_func=self.location_client.get_locations,
            filename_parts=filename_parts,
            format=format,
            data_folder=self.metadata_folder
        )
    
    def fetch_stations(
        self,
        datasetid=None,
        locationcategoryid=None,
        datacategoryid=None,
        datatypeid=None,
        extent=None,
        startdate=None,
        enddate=None,
        format=None
    ):
        params_extra = {
            "startdate": startdate,
            "enddate": enddate,
            "datasetid": datasetid,
            "locationcategoryid": locationcategoryid,
            "datacategoryid": datacategoryid,
            "datatypeid": datatypeid,
            "extent": extent,
        }
        filename_parts = [
            f"start-{startdate}" if startdate else None,
            f"end-{enddate}" if enddate else None,
            f"dataset-{datasetid}" if datasetid else None,
            f"locationcategory-{locationcategoryid}" if locationcategoryid else None,
            f"datacategory-{datacategoryid}" if datacategoryid else None,
            f"datatype-{datatypeid}" if datatypeid else None,
            f"extent-{extent}" if extent else None,
        ]
        return self._fetch_generic(
            base_name="stations",
            params_extra=params_extra,
            csv_func=self.station_client.get_stations_csv,
            df_func=self.station_client.get_stations_df,
            default_func=self.station_client.get_stations,
            filename_parts=filename_parts,
            format=format,
            data_folder=self.metadata_folder
        )