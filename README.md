## Data Source
This repository interacts with the [NOAA National Centers for Environmental Information (NCEI) Climate Data Online (CDO) Web Services](https://www.ncdc.noaa.gov/cdo-web/), which provide access to a wide range of climate and weather data. All data accessed through this API is publicly available from [NOAA NCEI](https://www.ncei.noaa.gov/).

## Data Description

The climate data can be retrieved from individual meteorological stations or from specific locations, which are essentially groups of such stations. The NOAA NCEI CDO Web Services provide access to a variety of datasets containing weather observations of different types. Users can also specify the start and end dates for the data they wish to retrieve.

## Services Access

To gain access to NOAA NCEI CDO Web Services, [request Web Services token](https://www.ncdc.noaa.gov/cdo-web/token) with your email address. An email will be sent with a unique token which will allow access RESTful services.

## Basic Use-Cases

In the folder `noaa-ncei-data-source/scripts`, there are scripts that demonstrate basic use cases of the NOAA NCEI CDO Web Services. These scripts retrieve data in `CSV` format in addition to the standard `JSON` API response format.

* `noaa-ncei-data-source/scripts/fetch_metadata_csv.py` fetches all metadata of the NOAA NCEI CDO Web Services.
* `noaa-ncei-data-source/scripts/fetch_us_zip_data_csv.py` fetches basic climate data from the Global Historical Climate Network Daily (GHCND) dataset from the US ZIP location.
* `noaa-ncei-data-source/scripts/fetch_salzburg_station_data_csv.py` fetches basic climate data from the Global Historical Climate Network Daily (GHCND) dataset for the Salzburg station.

For more information about CDO Web Services read the documentation for [CDO Web Services guide](https://www.ncdc.noaa.gov/cdo-web/webservices/v2).
