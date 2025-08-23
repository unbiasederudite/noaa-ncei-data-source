import time
import requests

class ApiClient:
    base_url = "https://www.ncei.noaa.gov/cdo-web/api/v2"

    def __init__(
            self,
            api_token=None,
            max_retries=5,
            retry_delay=5
    ):
        self.api_token = api_token
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    def get(self, endpoint, params=None):
        headers = {"token": self.api_token}
        url = f"{self.base_url}/{endpoint}"
        print(url)

        for attempt in range(1, self.max_retries + 1):
            try:
                response = requests.get(url, headers=headers, params=params, timeout=30)
                response.raise_for_status()
                return response.json()
            except requests.RequestException as e:
                print(f"(Attempt {attempt}) Request failed: {e}")
                if attempt == self.max_retries:
                    raise
                print(f"Retrying in {self.retry_delay} seconds...")
                time.sleep(self.retry_delay)

    def iter_pages(self, endpoint, params=None, limit=1000):
        if params is None:
            params = {}
        offset = 1
        while True:
            query_params = params.copy()
            query_params.update({"limit": limit, "offset": offset})
            response = self.get(endpoint, params=query_params)
            results = response.get("results", [])

            if not results:
                break

            yield results

            meta = response.get("metadata", {}).get("resultset", {})
            total_count = meta.get("count", 0)
            returned = len(results)

            if offset + returned > total_count:
                break

            offset += returned
            time.sleep(0.1)