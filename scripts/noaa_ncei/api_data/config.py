from dotenv import load_dotenv, find_dotenv
import os
from pathlib import Path

load_dotenv(find_dotenv())
API_TOKEN = os.getenv("API_TOKEN")

CURRENT_FILE = Path(__file__)

PARAMETER_TYPES = {
    "datasetid": str,
    "datatypeid": str,
    "datacategoryid": str,
    "startdate": str,  # format: YYYY-MM-DD
    "enddate": str,    # format: YYYY-MM-DD
    "units": str,
    "includemetadata": bool,
    "locationid": str,
    "stationid": str,
    "extent": str
}

FORMAT_TYPES = [None, 'csv']