from .base_api_client import ApiClient
import pandas as pd

class DatasetClient(ApiClient):
    api_endpoint = "datasets"
    def __init__(self, api_token=None):
        super().__init__(api_token)

    def get_datasets(
        self,
        datatypeid=None,
        locationid=None,
        stationid=None,
        startdate=None,
        enddate=None,
        sortfield=None,
        sortorder=None,
        limit=None,
    ):
        params = {}

        if datatypeid: params["datatypeid"] = datatypeid
        if locationid: params["locationid"] = locationid
        if stationid: params["stationid"] = stationid
        if startdate: params["startdate"] = startdate
        if enddate: params["enddate"] = enddate
        if sortfield: params["sortfield"] = sortfield
        if sortorder: params["sortorder"] = sortorder
        if limit: params["limit"] = limit

        return super().get_all_pages(self.api_endpoint, params)
    
    def get_datasets_df(self, **kwargs):
        return pd.DataFrame(self.get_datasets(**kwargs))

    def get_datasets_csv(self, file_path, **kwargs):
        df = self.get_datasets_df(**kwargs)
        df.to_csv(file_path, index=False)
        return file_path
    
    def get_dataset_info(self, dataset_id):
        endpoint = f"{self.api_endpoint}/{dataset_id}"
        return super().get(endpoint)
    
    def get_dataset_info_df(self, dataset_id):
        return pd.DataFrame([self.get_dataset_info(dataset_id)])

    def get_dataset_info_csv(self, dataset_id, file_path):
        df = self.get_dataset_info_df(dataset_id)
        df.to_csv(file_path, index=False)
        return file_path