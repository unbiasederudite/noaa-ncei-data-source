from noaa_ncei.api_data.base_api_data import ApiData
from noaa_ncei.api_client.base_api_client import ApiClient
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

        self.api_client = ApiClient(token)

    def fetch_data_categories(
        self,
        datasetid=None,
        locationid=None,
        stationid=None,
        startdate=None,
        enddate=None,
        format=None,
        limit=1000
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
            api_endpoint="datacategories",
            filename_parts=filename_parts,
            params_extra=params_extra,
            limit=limit,
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
        format=None,
        limit=1000
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
            api_endpoint="datatypes",
            filename_parts=filename_parts,
            params_extra=params_extra,
            limit=limit,
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
        format=None,
        limit=1000
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
            api_endpoint="datasets",
            filename_parts=filename_parts,
            params_extra=params_extra,
            limit=limit,
            format=format,
            data_folder=self.metadata_folder
        )
    
    def fetch_location_categories(
        self,
        datasetid=None,
        startdate=None,
        enddate=None,
        format=None,
        limit=1000
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
            api_endpoint="locationcategories",
            filename_parts=filename_parts,
            params_extra=params_extra,
            limit=limit,
            format=format,
            data_folder=self.metadata_folder
        )
    
    def fetch_locations(
        self,
        datasetid=None,
        locationcategoryid=None,
        startdate=None,
        enddate=None,
        format=None,
        limit=1000
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
            api_endpoint="locations",
            filename_parts=filename_parts,
            params_extra=params_extra,
            limit=limit,
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
        format=None,
        limit=1000
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
            api_endpoint="stations",
            filename_parts=filename_parts,
            params_extra=params_extra,
            limit=limit,
            format=format,
            data_folder=self.metadata_folder
        )