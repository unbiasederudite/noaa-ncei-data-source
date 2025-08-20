from .base_api_client import ApiClient
import pandas as pd

class StationClient(ApiClient):
    api_endpoint = "stations"
    def __init__(self, api_token=None):
        super().__init__(api_token)

    def get_stations(
        self,
        datasetid=None,
        locationid=None,
        datacategoryid=None,
        datatypeid=None,
        extent=None,
        startdate=None,
        enddate=None,
        sortfield=None,
        sortorder=None,
        limit=None,
    ):
        
        params = {}

        if datasetid: params["datasetid"] = datasetid
        if locationid: params["locationid"] = locationid
        if datacategoryid: params["datacategoryid"] = datacategoryid
        if datatypeid: params["datatypeid"] = datatypeid
        if extent: params["extent"] = extent
        if startdate: params["startdate"] = startdate
        if enddate: params["enddate"] = enddate
        if sortfield: params["sortfield"] = sortfield
        if sortorder: params["sortorder"] = sortorder
        if limit: params["limit"] = limit

        return super().get_all_pages(self.api_endpoint, params)
    
    def get_stations_df(self, **kwargs):
        return pd.DataFrame(self.get_stations(**kwargs))

    def get_stations_csv(self, file_path, **kwargs):
        df = self.get_stations_df(**kwargs)
        df.to_csv(file_path, index=False)
        return file_path
    
    def get_station_info(self, station_id):
        endpoint = f"{self.api_endpoint}/{station_id}"
        return super().get(endpoint)
    
    def get_station_info_df(self, station_id):
        return pd.DataFrame([self.get_station_info(station_id)])

    def get_station_info_csv(self, station_id, file_path):
        df = self.get_station_info_df(station_id)
        df.to_csv(file_path, index=False)
        return file_path