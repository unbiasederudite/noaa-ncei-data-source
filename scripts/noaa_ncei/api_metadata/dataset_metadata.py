from noaa_ncei.api_client.dataset_client import DatasetClient
from noaa_ncei.config import API_TOKEN, CURRENT_FILE
from noaa_ncei.base_api_data import ApiData

class DatasetMetadata(ApiData):
    dataset_metadata_folder = CURRENT_FILE.parent.parent.parent / "metadata" / "dataset"
    def __init__(self):
        self.params = {}
        self.dataset_client = DatasetClient(API_TOKEN)
    
    def fetch_datasets(
        self,
        datatypeid=None,
        locationid=None,
        stationid=None,
        startdate=None,
        enddate=None,
        format=None
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
            base_name="datasets",
            params_extra=params_extra,
            csv_func=self.dataset_client.get_datasets_csv,
            df_func=self.dataset_client.get_datasets_df,
            default_func=self.dataset_client.get_datasets,
            filename_parts=filename_parts,
            format=format,
            data_folder=self.dataset_metadata_folder
        )