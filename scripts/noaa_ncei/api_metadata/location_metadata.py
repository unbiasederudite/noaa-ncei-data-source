from noaa_ncei.api_client.location_client import LocationClient
from noaa_ncei.config import API_TOKEN, CURRENT_FILE
from noaa_ncei.base_api_data import ApiData

class LocationMetadata(ApiData):
    location_metadata_folder = CURRENT_FILE.parent.parent.parent / "metadata" / "location"
    def __init__(self):
        self.params = {}
        self.location_client = LocationClient(API_TOKEN)
   
    def fetch_locations(
        self,
        datasetid=None,
        locationcategoryid=None,
        startdate=None,
        enddate=None,
        format=None
    ):
        params_extra = {
            "startdate": startdate,
            "enddate": enddate,
            "datasetid": datasetid,
            "locationcategoryid": locationcategoryid,
        }
        filename_parts = [
            f"start-{startdate}" if startdate else None,
            f"end-{enddate}" if enddate else None,
            f"dataset-{datasetid}" if datasetid else None,
            f"locationcategory-{locationcategoryid}" if locationcategoryid else None
        ]
        return self._fetch_generic(
            base_name="locations",
            params_extra=params_extra,
            csv_func=self.location_client.get_locations_csv,
            df_func=self.location_client.get_locations_df,
            default_func=self.location_client.get_locations,
            filename_parts=filename_parts,
            format=format,
            data_folder=self.location_metadata_folder
        )