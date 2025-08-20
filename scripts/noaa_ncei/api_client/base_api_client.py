import requests
import os

class ApiClient:
    api_base_url = 'https://www.ncei.noaa.gov/cdo-web/api/v2/'
    def __init__(self, api_token=None):
        self.api_token = api_token or os.getenv("API_TOKEN")
        if not self.api_token:
            raise ValueError("Provide NOAA NCEI API token via parameter or API_TOKEN env var.")
        self.headers = {"token": api_token}

    def get(self, endpoint, params=None):
        url = f"{self.api_base_url}{endpoint}"
        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=10)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 400:
                return {"error": "Bad request", "details": response.text}
            elif response.status_code == 401:
                return {"error": "Unauthorized – invalid or missing token"}
            elif response.status_code == 404:
                return {"error": "Not found"}
            elif response.status_code == 429:
                return {"error": "Too many requests – rate limited"}
            elif response.status_code >= 500:
                return {"error": "Server error", "status": response.status_code}
            else:
                return {"error": "Unexpected response", "status": response.status_code, "details": response.text}
        except requests.exceptions.Timeout:
            return {"error": "Request timed out"}
        except requests.exceptions.RequestException as e:
            return {"error": f"Request failed: {str(e)}"}
        
    def get_all_pages(self, endpoint, params=None, limit=1000):
        if params is None:
            params = {}
        offset = 1
        all_results = []
        while True:
            params.update({"limit": limit, "offset": offset})
            response = self.get(endpoint, params=params)
            if "results" not in response:
                break
            all_results.extend(response["results"])
            meta = response.get("metadata", {}).get("resultset", {})
            if offset + limit > meta.get("count", 0):
                break
            offset += limit
        return all_results
