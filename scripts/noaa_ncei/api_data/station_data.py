from noaa_ncei.api_client.station_client import StationClient
from noaa_ncei.api_client.data_client import DataClient 
from noaa_ncei.api_client.dataset_client import DatasetClient
from noaa_ncei.api_client.data_type_client import DataTypeClient
from noaa_ncei.api_client.data_category_client import DataCategoryClient
from noaa_ncei.config import API_TOKEN, CURRENT_FILE
from noaa_ncei.base_api_data import ApiData

class StationData(ApiData):
    stations_data_folder = CURRENT_FILE.parent.parent.parent / "data" / "stations"
    def __init__(self, station_id):
        self.station_id = station_id
        self.params={"stationid": f"{self.station_id}"}

        self.station_data_folder = self.stations_data_folder / f"{self.station_id.replace(":", "-")}"

        self.station_client = StationClient(API_TOKEN)
        self.dataset_client = DatasetClient(API_TOKEN)
        self.data_category_client = DataCategoryClient(API_TOKEN)
        self.data_type_client = DataTypeClient(API_TOKEN)
        self.data_client = DataClient(API_TOKEN)
    
    def fetch_station_datasets(
        self,
        datatypeid=None,
        startdate=None,
        enddate=None,
        format=None
    ):
        params_extra = {
            "startdate": startdate,
            "enddate": enddate,
            "datatypeid": datatypeid,
        }
        filename_parts = [
            self.station_id,
            f"start-{startdate}" if startdate else None,
            f"end-{enddate}" if enddate else None,
            f"datatype-{datatypeid}" if datatypeid else None,
        ]
        return self._fetch_generic(
            base_name="datasets",
            params_extra=params_extra,
            csv_func=self.dataset_client.get_datasets_csv,
            df_func=self.dataset_client.get_datasets_df,
            default_func=self.dataset_client.get_datasets,
            filename_parts=filename_parts,
            format=format,
            data_folder=self.station_data_folder
        )

    def fetch_station_data_categories(
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
            self.station_id,
            f"start-{startdate}" if startdate else None,
            f"end-{enddate}" if enddate else None,
            f"dataset-{datasetid}" if datasetid else None,
        ]
        return self._fetch_generic(
            base_name="datacategories",
            params_extra=params_extra,
            csv_func=self.data_category_client.get_data_categories_csv,
            df_func=self.data_category_client.get_data_categories_df,
            default_func=self.data_category_client.get_data_categories,
            filename_parts=filename_parts,
            format=format,
            data_folder=self.station_data_folder
        )
    
    def fetch_station_data_types(
        self,
        datasetid=None,
        datacategoryid=None,
        startdate=None,
        enddate=None,
        format=None
    ):
        params_extra = {
            "startdate": startdate,
            "enddate": enddate,
            "datasetid": datasetid,
            "datacategoryid": datacategoryid,
        }
        filename_parts = [
            self.station_id,
            f"start-{startdate}" if startdate else None,
            f"end-{enddate}" if enddate else None,
            f"dataset-{datasetid}" if datasetid else None,
            f"datacategory-{datacategoryid}" if datacategoryid else None,
        ]
        return self._fetch_generic(
            base_name="datatypes",
            params_extra=params_extra,
            csv_func=self.data_type_client.get_data_types_csv,
            df_func=self.data_type_client.get_data_types_df,
            default_func=self.data_type_client.get_data_types,
            filename_parts=filename_parts,
            format=format,
            data_folder=self.station_data_folder
        )
    
    def fetch_station_data(
        self,
        datasetid,
        startdate,
        enddate,
        datatypeid=None,
        units=None,
        includemetadata=None,
        format=None
    ):
        if not datasetid:
            raise ValueError("datasetid is required to fetch data")
        
        if not startdate:
            raise ValueError("startdate is required to fetch data")
        
        if not enddate:
            raise ValueError("enddate is required to fetch data")
        
        params_extra = {
            "startdate": startdate,
            "enddate": enddate,
            "datasetid": datasetid,
            "datatypeid": datatypeid,
            "units": units,
            "includemetadata": includemetadata,
        }
        filename_parts = [
            self.station_id,
            f"start-{startdate}",
            f"end-{enddate}",
            f"dataset-{datasetid}",
            f"datatype-{datatypeid}" if datatypeid else None,
            f"units-{units}" if units else None,
        ]
        return self._fetch_generic(
            base_name="data",
            params_extra=params_extra,
            csv_func=self.data_client.get_data_csv,
            df_func=self.data_client.get_data_df,
            default_func=self.data_client.get_data,
            filename_parts=filename_parts,
            format=format,
            data_folder=self.station_data_folder
        )
    
    def fetch_station_info(
        self,
        format=None
    ):
        filename_parts = [
            self.station_id,
        ]
        return self._fetch_info_generic(
            base_name="station",
            id_key="stationid",
            csv_func=self.station_client.get_station_info_csv,
            df_func=self.station_client.get_station_info_df,
            default_func=self.station_client.get_station_info,
            filename_parts=filename_parts,
            format=format,
            data_folder=self.station_data_folder
        )