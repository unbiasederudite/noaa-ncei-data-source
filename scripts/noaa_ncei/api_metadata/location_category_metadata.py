from noaa_ncei.api_client.location_category_client import LocationCategoryClient
from noaa_ncei.config import API_TOKEN, CURRENT_FILE
from noaa_ncei.base_api_data import ApiData

class LocationCategoryMetadata(ApiData):
    location_category_metadata_folder = CURRENT_FILE.parent.parent.parent / "metadata" / "location_category"
    def __init__(self):
        self.params = {}
        self.location_category_client = LocationCategoryClient(API_TOKEN)
    
    def fetch_location_categories(
        self,
        datasetid=None,
        startdate=None,
        enddate=None,
        format=None
    ):
        params_extra = {
            "startdate": startdate,
            "enddate": enddate,
            "datasetid": datasetid,
        }
        filename_parts = [
            f"start-{startdate}" if startdate else None,
            f"end-{enddate}" if enddate else None,
            f"dataset-{datasetid}" if datasetid else None,
        ]
        return self._fetch_generic(
            base_name="location_categories",
            params_extra=params_extra,
            csv_func=self.location_category_client.get_location_categories_csv,
            df_func=self.location_category_client.get_location_categories_df,
            default_func=self.location_category_client.get_location_categories,
            filename_parts=filename_parts,
            format=format,
            data_folder=self.location_category_metadata_folder
        )