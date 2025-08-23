from noaa_ncei.api_client.base_api_client import ApiClient
from noaa_ncei.api_data.config import API_TOKEN, CURRENT_FILE
from noaa_ncei.api_data.base_api_data import ApiData
import os

class StationData(ApiData):
    def __init__(
            self, 
            station_id, 
            stations_data_folder = os.path.join(CURRENT_FILE.parents[3], "data", "stations"),
            **kwargs
    ):
        super().__init__(**kwargs)
        self.station_dataset, self.station_id = station_id.split(':', 1)
        self.params = {"stationid": f"{self.station_dataset}:{self.station_id}"}

        self.stations_data_folder = stations_data_folder
        self.station_data_folder = os.path.join(self.stations_data_folder, self.station_id)

        token = self.api_token or API_TOKEN

        self.api_client = ApiClient(token)

    def fetch_station_datasets(self, datatypeid=None, startdate=None, enddate=None, format=None, limit=1000):
        params_extra = {
            "startdate": startdate,
            "enddate": enddate,
            "datatypeid": datatypeid,
        }
        filename_parts = [
            f"start-{startdate}" if startdate else None,
            f"end-{enddate}" if enddate else None,
            f"datatype-{datatypeid}" if datatypeid else None,
        ]
        return self._fetch_generic(
            api_endpoint="datasets",
            filename_parts=filename_parts,
            params_extra=params_extra,
            limit=limit,
            format=format,
            data_folder=self.station_data_folder)

    def fetch_station_data_categories(self, datasetid=None, startdate=None, enddate=None, format=None, limit=1000):
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
            api_endpoint="datacategories",
            filename_parts=filename_parts,
            params_extra=params_extra,
            limit=limit,
            format=format,
            data_folder=self.station_data_folder)

    def fetch_station_data_types(self, datasetid=None, datacategoryid=None, startdate=None, enddate=None, format=None, limit=1000):
        params_extra = {
            "startdate": startdate,
            "enddate": enddate,
            "datasetid": datasetid,
            "datacategoryid": datacategoryid,
        }
        filename_parts = [
            f"start-{startdate}" if startdate else None,
            f"end-{enddate}" if enddate else None,
            f"dataset-{datasetid}" if datasetid else None,
            f"datacategory-{datacategoryid}" if datacategoryid else None,
        ]
        return self._fetch_generic(
            api_endpoint="datatypes",
            filename_parts=filename_parts,
            params_extra=params_extra,
            limit=limit,
            format=format,
            data_folder=self.station_data_folder)

    def fetch_station_data(self, datasetid, startdate, enddate, datatypeid=None, units=None, includemetadata=None, format=None, limit=1000):
        if not datasetid:
            raise ValueError("datasetid is required to fetch data")
        
        if not startdate:
            raise ValueError("startdate is required to fetch data")
        
        if not enddate:
            raise ValueError("enddate is required to fetch data")

        params_extra = {
            "startdate": startdate,
            "enddate": enddate,
            "datasetid": datasetid,
            "datatypeid": datatypeid,
            "units": units,
            "includemetadata": includemetadata,
        }
        filename_parts = [
            f"start-{startdate}",
            f"end-{enddate}",
            f"dataset-{datasetid}",
            f"datatype-{datatypeid}" if datatypeid else None,
            f"units-{units}" if units else None,
        ]
        return self._fetch_generic(
            api_endpoint="data",
            filename_parts=filename_parts,
            params_extra=params_extra,
            limit=limit,
            format=format,
            data_folder=self.station_data_folder)