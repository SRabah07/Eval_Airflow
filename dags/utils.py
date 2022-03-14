import os
import requests
import json
import logging


class APIHelper:
    def __init__(self, api_key):
        self.api_key = api_key
        if self.api_key is None:
            raise Exception("API Key is required!")

        self.url = "https://api.openweathermap.org/data/2.5/weather"
        logging.info(f"API URL = {self.url}")

    def makeRequest(self, city: str):
        if city is None:
            raise Exception('City is required!')
        logging.info(f"Get weather of city: {city}")
        params = {
            "q": city,
            "appid": self.api_key
        }

        result = requests.get(self.url, params=params)

        if result.status_code == 200:
            content = result.content.decode("utf-8")
            logging.debug(f'\nWeather on city: {city} is: \n {content}')
            return json.loads(content)
        else:
            logging.error(f'Error request city of: {city}. {result.content}')
            raise Exception(result.content.decode("utf-8"))
