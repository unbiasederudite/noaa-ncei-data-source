from .base_api_client import ApiClient
import pandas as pd

class DataCategoryClient(ApiClient):
    api_endpoint = "datacategories"
    def __init__(self, api_token=None):
        super().__init__(api_token)

    def get_data_categories(
        self,
        datasetid=None,
        locationid=None,
        stationid=None,
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
        if startdate: params["startdate"] = startdate
        if enddate: params["enddate"] = enddate
        if sortfield: params["sortfield"] = sortfield
        if sortorder: params["sortorder"] = sortorder
        if limit: params["limit"] = limit

        return super().get_all_pages(self.api_endpoint, params)
    
    def get_data_categories_df(self, **kwargs):
        return pd.DataFrame(self.get_data_categories(**kwargs))

    def get_data_categories_csv(self, file_path, **kwargs):
        df = self.get_data_categories_df(**kwargs)
        df.to_csv(file_path, index=False)
        return file_path
    
    def get_data_category_info(self, datacategory_id):
        endpoint = f"{self.api_endpoint}/{datacategory_id}"
        return super().get(endpoint)
    
    def get_data_category_info_df(self, datacategory_id):
        return pd.DataFrame([self.get_data_category_info(datacategory_id)])

    def get_data_category_info_csv(self, datacategory_id, file_path):
        df = self.get_data_category_info_df(datacategory_id)
        df.to_csv(file_path, index=False)
        return file_path