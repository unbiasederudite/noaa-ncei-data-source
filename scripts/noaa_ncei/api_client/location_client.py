from .base_api_client import ApiClient
import pandas as pd

class LocationClient(ApiClient):
    api_endpoint = "locations"
    def __init__(self, api_token=None):
        super().__init__(api_token)

    def get_locations(
        self,
        datasetid=None,
        locationcategoryid=None,
        datacategoryid=None,
        startdate=None,
        enddate=None,
        sortfield=None,
        sortorder=None,
        limit=None,
    ):
        
        params = {}

        if datasetid: params["datasetid"] = datasetid
        if locationcategoryid: params["locationcategoryid"] = locationcategoryid
        if datacategoryid: params["datacategoryid"] = datacategoryid
        if startdate: params["startdate"] = startdate
        if enddate: params["enddate"] = enddate
        if sortfield: params["sortfield"] = sortfield
        if sortorder: params["sortorder"] = sortorder
        if limit: params["limit"] = limit

        return super().get_all_pages(self.api_endpoint, params)
    
    def get_locations_df(self, **kwargs):
        return pd.DataFrame(self.get_locations(**kwargs))

    def get_locations_csv(self, file_path, **kwargs):
        df = self.get_locations_df(**kwargs)
        df.to_csv(file_path, index=False)
        return file_path
    
    def get_location_info(self, location_id):
        endpoint = f"{self.api_endpoint}/{location_id}"
        return super().get(endpoint)
    
    def get_location_info_df(self, location_id):
        return pd.DataFrame([self.get_location_info(location_id)])

    def get_location_info_csv(self, location_id, file_path):
        df = self.get_location_info_df(location_id)
        df.to_csv(file_path, index=False)
        return file_path