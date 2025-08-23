from noaa_ncei.api_data.config import PARAMETER_TYPES, FORMAT_TYPES
from noaa_ncei.api_client.base_api_client import ApiClient
from pathlib import Path
import os 

class ApiData(ApiClient):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def check_parameter_types(params: dict):
        for key, value in params.items():
            if value is not None:
                expected_type = PARAMETER_TYPES.get(key)
                if expected_type and not isinstance(value, expected_type):
                    raise TypeError(
                        f"Parameter '{key}' has incorrect type. "
                        f"Expected {expected_type.__name__}, got {type(value).__name__}."
                    )

    @staticmethod
    def build_filename(base, *args, ext="csv"):
        parts = [base]
        cleaned_args = [
            str(arg).replace('&', '_').replace(':', '-') 
            for arg in args if arg is not None
        ]
        cleaned_args.sort(reverse=True)
        parts.extend(cleaned_args)
        filename = "_".join(parts) + f".{ext}"
        return filename
    
    def _fetch_generic(
        self,
        base_name,
        params_extra=None,
        csv_func=None,
        df_func=None,
        default_func=None,
        filename_parts=None,
        format=None,
        data_folder=None
    ):
        params = self.params.copy()
        if params_extra:
            params.update({k: v for k, v in params_extra.items() if v is not None})

        self.check_parameter_types(params)

        allowed_formats = FORMAT_TYPES
        if format not in allowed_formats:
            raise ValueError(f"Invalid format: {format}. Allowed values are: {allowed_formats}")
        
        if format == 'csv':
            if data_folder is None:
                raise ValueError("data_folder must be provided when saving CSV output")
            Path(data_folder).mkdir(parents=True, exist_ok=True)
            filename = self.build_filename(base_name, *filename_parts)
            file_path = os.path.join(data_folder, filename)
            return csv_func(file_path=file_path, **params)

        if format == 'df':
            return df_func(**params)

        return default_func(**params)
    
    def _fetch_info_generic(
        self,
        base_name,
        id_key,
        csv_func=None,
        df_func=None,
        default_func=None,
        filename_parts=None,
        format=None,
        data_folder=None
    ):
        allowed_formats = FORMAT_TYPES
        if format not in allowed_formats:
            raise ValueError(f"Invalid format: {format}. Allowed values are: {allowed_formats}")
        
        Path(data_folder).mkdir(parents=True, exist_ok=True)
        filename = self.build_filename(base_name, *filename_parts)
        file_path = os.path.join(data_folder, filename)

        identifier = self.params[id_key]

        if format == 'csv':
            if data_folder is None:
                raise ValueError("data_folder must be provided when saving CSV output")
            return csv_func(identifier, file_path)

        if format == 'df':
            return df_func(identifier)

        return default_func(identifier)