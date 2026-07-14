import os
import requests

class PrometheusProbe:

    def __init__(self):
        self.prometheus_url = os.environ["PROMETHEUS_URL"]

    def execute_query(self, query):
        response = requests.get(f"http://{self.prometheus_url}/api/v1/query", params={'query': query})
        print(response.json()["data"]["result"][0]["value"][1])
