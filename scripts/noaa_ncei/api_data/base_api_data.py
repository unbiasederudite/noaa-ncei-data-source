from noaa_ncei.api_data.config import PARAMETER_TYPES, FORMAT_TYPES
from noaa_ncei.api_client.base_api_client import ApiClient
from pathlib import Path
import os 
import csv

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
    def build_filename(base, *args, ext):
        parts = [base]
        cleaned_args = [
            str(arg).replace('&', '_').replace(':', '-') 
            for arg in args if arg is not None
        ]
        cleaned_args.sort(reverse=True)
        parts.extend(cleaned_args)
        filename = "_".join(parts) + f".{ext}"
        return filename

    @staticmethod 
    def save_to_csv(iterator, file_path, fieldnames=None):
        first_chunk = True
        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            writer = None
            for chunk in iterator:
                if chunk:
                    if first_chunk:
                        if not fieldnames:
                            fieldnames = list(chunk[0].keys())
                        writer = csv.DictWriter(f, fieldnames=fieldnames)
                        writer.writeheader()
                        first_chunk = False
                    for item in chunk:
                        writer.writerow(item)
        return file_path

    def _fetch_generic(
        self,
        api_endpoint,
        filename_parts=None,
        params_extra=None,
        limit=1000,
        format=None,
        data_folder=None
    ):  
        params = self.params.copy()
        if params_extra:
            params.update({k: v for k, v in params_extra.items() if v is not None})
        print(params)
        self.check_parameter_types(params)

        iterator = self.iter_pages(api_endpoint, params, limit)

        allowed_formats = FORMAT_TYPES
        if format not in allowed_formats:
            raise ValueError(f"Invalid format: {format}. Allowed values are: {allowed_formats}")
        
        if format == 'csv':
            if data_folder is None:
                raise ValueError("data_folder must be provided when saving CSV output")
            Path(data_folder).mkdir(parents=True, exist_ok=True)
            filename = self.build_filename(api_endpoint, *filename_parts, ext=format)
            file_path = os.path.join(data_folder, filename)
            return self.save_to_csv(iterator, file_path)

        return iterator