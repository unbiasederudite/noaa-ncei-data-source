from .base_api_client import ApiClient
import pandas as pd

class DataTypeClient(ApiClient):
    api_endpoint = "datatypes"
    def __init__(self, api_token=None):
        super().__init__(api_token)

    def get_data_types(
        self,
        datasetid=None,
        locationid=None,
        stationid=None,
        datacategoryid=None,
        startdate=None,
        enddate=None,
        sortfield=None,
        sortorder=None,
        limit=None,
    ):
        params = {}

        if datasetid: params["datasetid"] = datasetid
        if locationid: params["locationid"] = locationid
        if stationid: params["stationid"] = stationid
        if datacategoryid: params["datacategoryid"] = datacategoryid
        if startdate: params["startdate"] = startdate
        if enddate: params["enddate"] = enddate
        if sortfield: params["sortfield"] = sortfield
        if sortorder: params["sortorder"] = sortorder
        if limit: params["limit"] = limit

        return super().get_all_pages(self.api_endpoint, params)
    
    def get_data_types_df(self, **kwargs):
        return pd.DataFrame(self.get_data_types(**kwargs))

    def get_data_types_csv(self, file_path, **kwargs):
        df = self.get_data_types_df(**kwargs)
        df.to_csv(file_path, index=False)
        return file_path

    def get_data_type_info(self, datatype_id):
        endpoint = f"{self.api_endpoint}/{datatype_id}"
        return super().get(endpoint)
    
    def get_data_type_info_df(self, datatype_id):
        return pd.DataFrame([self.get_data_type_info(datatype_id)])

    def get_data_type_info_csv(self, datatype_id, file_path):
        df = self.get_data_type_info_df(datatype_id)
        df.to_csv(file_path, index=False)
        return file_path