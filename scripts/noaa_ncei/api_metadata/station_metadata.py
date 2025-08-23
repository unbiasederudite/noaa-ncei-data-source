from noaa_ncei.api_client.station_client import StationClient
from noaa_ncei.config import API_TOKEN, CURRENT_FILE
from noaa_ncei.base_api_data import ApiData

class StationMetadata(ApiData):
    station_metadata_folder = CURRENT_FILE.parent.parent.parent / "metadata" / "station"
    def __init__(self):
        self.params = {}
        self.station_client = StationClient(API_TOKEN)
    
    def fetch_stations(
        self,
        datasetid=None,
        locationcategoryid=None,
        datacategoryid=None,
        datatypeid=None,
        extent=None,
        startdate=None,
        enddate=None,
        format=None
    ):
        params_extra = {
            "startdate": startdate,
            "enddate": enddate,
            "datasetid": datasetid,
            "locationcategoryid": locationcategoryid,
            "datacategoryid": datacategoryid,
            "datatypeid": datatypeid,
            "extent": extent,
        }
        filename_parts = [
            f"start-{startdate}" if startdate else None,
            f"end-{enddate}" if enddate else None,
            f"dataset-{datasetid}" if datasetid else None,
            f"locationcategory-{locationcategoryid}" if locationcategoryid else None,
            f"datacategory-{datacategoryid}" if datacategoryid else None,
            f"datatype-{datatypeid}" if datatypeid else None,
            f"extent-{extent}" if extent else None,
        ]
        return self._fetch_generic(
            base_name="stations",
            params_extra=params_extra,
            csv_func=self.station_client.get_stations_csv,
            df_func=self.station_client.get_stations_df,
            default_func=self.station_client.get_stations,
            filename_parts=filename_parts,
            format=format,
            data_folder=self.station_metadata_folder
        )