from noaa_ncei.api_client.data_type_client import DataTypeClient
from noaa_ncei.config import API_TOKEN, CURRENT_FILE
from noaa_ncei.base_api_data import ApiData

class DataTypeMetadata(ApiData):
    data_type_metadata_folder = CURRENT_FILE.parent.parent.parent / "metadata" / "data_type"
    def __init__(self):
        self.params = {}
        self.data_type_client = DataTypeClient(API_TOKEN)
    
    def fetch_data_types(
        self,
        datasetid=None,
        locationid=None,
        stationid=None,
        datacategoryid=None,
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
            "datacategoryid": datacategoryid,
        }
        filename_parts = [
            f"start-{startdate}" if startdate else None,
            f"end-{enddate}" if enddate else None,
            f"dataset-{datasetid}" if datasetid else None,
            f"location-{locationid}" if locationid else None,
            f"station-{stationid}" if stationid else None,
            f"datacategory-{datacategoryid}" if datacategoryid else None,
        ]
        return self._fetch_generic(
            base_name="data_types",
            params_extra=params_extra,
            csv_func=self.data_type_client.get_data_types_csv,
            df_func=self.data_type_client.get_data_types_df,
            default_func=self.data_type_client.get_data_types,
            filename_parts=filename_parts,
            format=format,
            data_folder=self.data_type_metadata_folder
        )