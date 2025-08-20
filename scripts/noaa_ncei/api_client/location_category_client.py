from .base_api_client import ApiClient
import pandas as pd

class LocationCategoryClient(ApiClient):
    api_endpoint = "locationcategories"
    def __init__(self, api_token=None):
        super().__init__(api_token)

    def get_location_categories(
        self,
        datasetid=None,
        startdate=None,
        enddate=None,
        sortfield=None,
        sortorder=None,
        limit=None,
    ):
        params = {}

        if datasetid: params["datasetid"] = datasetid
        if startdate: params["startdate"] = startdate
        if enddate: params["enddate"] = enddate
        if sortfield: params["sortfield"] = sortfield
        if sortorder: params["sortorder"] = sortorder
        if limit: params["limit"] = limit

        return super().get_all_pages(self.api_endpoint, params)
    
    def get_location_categories_df(self, **kwargs):
        return pd.DataFrame(self.get_location_categories(**kwargs))

    def get_location_categories_csv(self, file_path, **kwargs):
        df = self.get_location_categories_df(**kwargs)
        df.to_csv(file_path, index=False)
        return file_path
    
    def get_location_category_info(self, locationcategory_id):
        endpoint = f"{self.api_endpoint}/{locationcategory_id}"
        return super().get(endpoint)
    
    def get_location_category_info_df(self, locationcategory_id):
        return pd.DataFrame([self.get_location_category_info(locationcategory_id)])

    def get_location_category_info_csv(self, locationcategory_id, file_path):
        df = self.get_location_category_info_df(locationcategory_id)
        df.to_csv(file_path, index=False)
        return file_path