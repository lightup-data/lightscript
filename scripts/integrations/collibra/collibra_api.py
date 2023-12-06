import base64
import logging
import os

import requests
from dotenv import load_dotenv

load_dotenv(".env")

logger = logging.getLogger(__name__)


class CollibraAPI:
    def __init__(self, log_level=logging.INFO):
        self.username = os.environ["COLLIBRA_USERNAME"]
        self.password = os.environ["COLLIBRA_PASSWORD"]
        self.rest_url = os.environ["COLLIBRA_REST_URL"]
        self.headers = {
            "Accept": "application/json",
            "Authorization": self.basic_auth_header(self.username, self.password),
        }

    @staticmethod
    def basic_auth_header(username, password):
        combined = f"{username}:{password}"
        combined_bytes = combined.encode("utf-8")
        encoded_string = base64.b64encode(combined_bytes)
        auth_header = f"Basic {encoded_string.decode('utf-8')}"
        return auth_header

    def request(self, method, endpoint, data=None):
        url = f"{self.rest_url}/{endpoint}"
        logger.info(f"METHOD: {method} {url}")
        try:
            if method == "GET":
                response = requests.get(url, headers=self.headers)
            elif method == "POST":
                response = requests.post(url, headers=self.headers, json=data)
            elif method == "PUT":
                response = requests.put(url, headers=self.headers, json=data)
            elif method == "DELETE":
                response = requests.delete(url, headers=self.headers)
            elif method == "PATCH":
                response = requests.patch(url, headers=self.headers, json=data)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            response.raise_for_status()

            return response.json() if response.content else None

        except requests.RequestException as e:
            logger.error(f"Error during {method} request to {endpoint}: {str(e)}")
            return None

    def get(self, endpoint):
        return self.request("GET", endpoint)

    def post(self, endpoint, data):
        return self.request("POST", endpoint, data)

    def delete(self, endpoint):
        return self.request("DELETE", endpoint)

    def patch(self, endpoint, data):
        return self.request("PATCH", endpoint, data)

    def put(self, endpoint, data):
        return self.request("PUT", endpoint, data)
