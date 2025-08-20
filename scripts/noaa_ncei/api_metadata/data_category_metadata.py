from noaa_ncei.api_client.data_category_client import DataCategoryClient
from noaa_ncei.config import API_TOKEN, CURRENT_FILE
from noaa_ncei.base_api_data import ApiData

class DataCategoryMetadata(ApiData):
    data_category_metadata_folder = CURRENT_FILE.parent.parent.parent / "metadata" / "data_category"
    def __init__(self):
        self.params = {}
        self.data_category_client = DataCategoryClient(API_TOKEN)
    
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
            data_folder=self.data_category_metadata_folder
        )