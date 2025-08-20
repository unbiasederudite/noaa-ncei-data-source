from .base_api_client import ApiClient
import pandas as pd

class DataClient(ApiClient):
    api_endpoint = "data"
    def __init__(self, api_token=None):
        super().__init__(api_token)

    def get_data(
        self,
        datasetid,
        startdate,
        enddate,
        datatypeid=None,
        locationid=None,
        stationid=None,
        units=None,
        sortfield=None,
        sortorder=None,
        limit=None,
        includemetadata=None,
    ):
        if not datasetid:
            raise ValueError("datasetid is required to fetch data")
        
        if not startdate:
            raise ValueError("startdate is required to fetch data")
        
        if not enddate:
            raise ValueError("enddate is required to fetch data")

        params = {
            "datasetid": datasetid,
            "startdate": startdate,
            "enddate": enddate
        }

        if datatypeid: params["datatypeid"] = datatypeid
        if locationid: params["locationid"] = locationid
        if stationid: params["stationid"] = stationid
        if units: params["units"] = units
        if sortfield: params["sortfield"] = sortfield
        if sortorder: params["sortorder"] = sortorder
        if limit: params["limit"] = limit
        if includemetadata: params["includemetadata"] = includemetadata

        return super().get_all_pages(self.api_endpoint, params)
    
    def get_data_df(self, **kwargs):
        return pd.DataFrame(self.get_data(**kwargs))

    def get_data_csv(self, file_path, **kwargs):
        df = self.get_data_df(**kwargs)
        df.to_csv(file_path, index=False)
        return file_path
