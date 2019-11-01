"""
TICKETMASTER API DATA PULL
"""


import pandas as pd
from unidecode import unidecode
import pymysql
from fuzzywuzzy import fuzz
from dataclasses import dataclass
import requests
from loguru import logger

logger.add("ticketmaster_log_{time}", format="{time} {level} {module} {name} {message}")

logger.info("Ticketmaster API pull start")

EVENT_BASE_URL = "https://app.ticketmaster.com/discovery/v2/events.json"
ARTIST_COUNT = 250
API_RETRIES = 3

print(API_RETRIES)


@dataclass()
class ApiKey:
    key: str
    count: int


@dataclass()
class Artist:
    id: str
    keyword: str
    spotify: str


api_keys = [
    ApiKey(key="83sdXVyv4k3NnuGCIvk5nAHE3NSWddg7", count=125),
    ApiKey(key="2C4llrNfIrGgEZxAft1QuJ5bpbS3SdpF", count=125),
]


def safeget(dct, *keys):
    for key in keys:
        try:
            dct = dct[key]
        except KeyError:
            return None
    return dct

class TicketmasterAPI:
    def __init__(self, keys, retries=3):
        self.keys_iter = iter(keys)
        self.key = next(self.keys_iter)
        self.retries = retries

    def get_json(self, artist_keyword):
        if self.key is None:
            logger.info("Out of keys")
            return None, True
            # TODO: Return error code here?
        for _ in range(self.retries):
            r = requests.get(
                EVENT_BASE_URL,
                params={"size": 25, "keyword": artist_keyword, "apikey": self.key},
            )
            if r.status_code == 200:  #Good response
                logger.info(f"Status code {r.status_code} artist: {artist_keyword}")
                return r.json(), False
            if r.status_code == 429 or r.status_code == 401:  #Too many requests or invalid key; get a new key
                logger.info(f"Status code {r.status_code} response: {r.content}")
                self.key = next(self.keys_iter, None)
                continue
            logger.info(f"Unexpected status code {r.status_code} response {r.content}")
            return None, True

    def persist_event_data(self, artist_keyword, spotify_artist):
        json_data = self.get_json(artist_keyword)
        self.persist_from_json(json_data, spotify_artist)


test = TicketmasterAPI(api_keys)

test.get_json('SALES')