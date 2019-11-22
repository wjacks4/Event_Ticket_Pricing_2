# WORK IN PROCESS!!!!!
# !/usr/bin/env python3
# The 'shebang' line should always be in the first line of the file

# Note:  A famous guy said "Comments are deoderant on smelly code". In general, if you have to comment something, then
# the code is unclear. I have added code to explain some maybe-new stuff or rules for you. You may delete all of my
# comments and I think the code will stand on its own.
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
import json
import util
from util import safeget
import time


# If you are writing code this sophisticated there is no reason for you not to be using a real logger.
# This sets up the logger. After this, you just pick a logging level and send it strings and it does the rest.
logger.add("ticketmaster_log_{time}", format="{time} {level} {module} {name} {message}")

logger.info("Ticketmaster API pull start")

# Constants in python should be ALL CAPS; you don't need a comment because it is very clear
# what's a global constant when all CAPS at the top of a file
EVENT_BASE_URL = "https://app.ticketmaster.com/discovery/v2/events.json"
ARTIST_COUNT = 250
API_RETRIES = 3


@dataclass()
class ApiKey:
    key: str
    count: int


@dataclass()
class Artist:
    id: str
    keyword: str
    spotify: str


api_keys = []

api_keys.append(ApiKey(key="83sdXVyv4k3NnuGCIvk5nAHE3NSWddg7", count=5))
api_keys.append(ApiKey(key="2C4llrNfIrGgEZxAft1QuJ5bpbS3SdpF", count=5))


def data_fetch_pymysql():
    connection = pymysql.connect(
        host="ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com",
        user="tickets_user",
        password="tickets_pass",
        db="tickets_db",
    )
    return pd.read_sql(
        "SELECT * FROM ARTISTS_WITH_EVENTS order by event_count desc, current_followers desc",
        con=connection,
    )


# Safe dictionary/list lookup so you don't have "try:except" everywhere
# def safeget(dct, *keys):
#    for key in keys:
#        try:
#            dct = dct[key]
#        except KeyError:
#            return None
#    return dct


df = data_fetch_pymysql()

# print(df.head(10))

class TEST:
    
    def __init__(self, keys, retries=3):
        self.keys_iter = iter(keys)
        self.key_obj = next(self.keys_iter)
        self.retries = retries
        

    def get_json(self, artist_keyword):
    
        for _ in range(self.retries):
            r = requests.get(
                EVENT_BASE_URL,
                params={"size": 25, "keyword": artist_keyword, "apikey": self.key_obj.key},
            )
            
            if r.status_code == 200:  #Good response
                logger.info(f"Status code {r.status_code} artist: {artist_keyword} key:{self.key_obj.key}")
                return 'STATUS CODE GOOD - SHOULD EXIT - '
                # return r.json(), False
            if r.status_code == 429 or r.status_code == 401:  #Too many requests or invalid key; get a new key
                logger.info(f"Status code {r.status_code} response: {r.content}")
                self.key_obj = next(self.keys_iter, None)
                logger.info(f"New key is {self.key_obj.key}")
                continue
                
            # logger.info(f"Unexpected status code {r.status_code} response {r.content}")
            return None, True    

    def persist_from_json(self, events_json, spotify_artist):
        for event in safeget(events_json, "_embedded", "events"):
            if name_ok(safeget(event, "name"), spotify_artist):
                venue_dict = safeget(event, "_embedded", "venues", 0, "name")
                # print(venue_dict)
                
  

def class_caller(artists_df):
    
    for artist_dat in artists_df.iterrows():

        spotify_artist = artist_dat[1]['artist']
        artist_encode = spotify_artist.encode('utf-8')
        artist_decode = unidecode(str(artist_encode, encoding = "utf-8"))
        artist_keyword = artist_decode.replace(" ", "+")

        test_instance = TEST(api_keys)
        # time.sleep(1)
        test_instance.get_json(artist_keyword)
        # print(each_instance)
        


artists_df = data_fetch_pymysql().head(10)
class_caller(artists_df)


#test_instance = TEST(api_keys)

#print(test_instance.get_json())

# test_instance.persist_from_json(test_instance.get_json()[1], test_instance.get_json()[0])




            


# All logic for determining name validity is in one spot in case you want to use something other
# than fuzzywuzzy or in case you want to use validated names. This returns boolean so you can use it as go/no-go test.
# (By the way - fuzzywuzzy is totally cool. I've never seen it and it was really clever to use here)
def name_ok(name, target):
    fuzz_partial = fuzz.partial_ratio(target.lower(), name.lower())
    fuzz_ratio = fuzz.ratio(target.lower(), name.lower())
    return (fuzz_ratio + fuzz_partial) > 150


class TicketmasterAPI:
    def __init__(self, keys, retries=3):
        self.keys_iter = iter(keys)
        self.key = next(self.keys_iter)
        self.retries = retries

    def persist_event_data(self, artist_keyword, spotify_artist):
        json_data = self.get_json(artist_keyword)
        self.persist_from_json(json_data, spotify_artist)

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

    def persist_from_json(self, events_json, spotify_artist):
        for event in safeget(events_json, "_embedded", "events"):
            if name_ok(safeget(event, "name"), spotify_artist):
                venue_dict = safeget(event, "_embedded", "venues", 0, "name")
                city = safeget(event, "_embedded", "venues", 0, "city", "name")
                event_state = safeget(event, "_embedded", "venues", 0, "state", "name")
                date_UTC = (
                    safeget(event, "dates", "start", "dateTime")
                    .replace("Z", "")
                    .replace("T", " ")
                )
                event_sale_start = safeget(event, "sales", "public", "startDateTime")
                event_lowest_price = safeget(event, "priceRanges", 0, "min")
                event_highest_price = safeget(event, "priceRanges", 0, "max")


# def pull_from_api(artist_keyword):
#     update_key = False
#     retry = API_RETRIES
#     while retry > 0:
#         key = api_key.send(update_key)
#         r = requests.get(EVENT_BASE_URL, params={'size':25, 'keyword': artist_keyword, 'apikey': key})
#         if r.status_code == '200':
#             return r.json()
#         # if r.status_code
#         #     401: Invalid API key
#         #     429: Too many requests
#         #     Rate limit is in the header?? Let's look at a header - yes it is
#         retry -= 1


# I refactored the 'argument_in' to 'argument' since python arguments are *always* inputs. If you somehow
# are using arguments for outputs, there is a serious architecture problem.
# def ticketmaster_event_pull(spotify_artist, artist_keyword):
#     # SJ: This is where next(api_key) pulls the next available key. How many are available is managed by get_api_key()
#     access_string = (
#         EVENT_BASE_URL + next(api_key) + "&size=25&keyword=" + artist_keyword
#     )
#
#     # I'm glad you figured out how to use urllib...but there is a MUCH better http package you should be using: requests
#     # I converted this to requests so you can appreciate the beauty... :-)
#
#     raw_Dat = urllib.request.urlopen(access_string)
#     encoded_Dat = raw_Dat.read().decode("utf-8", "ignore")
#     json_Dat = json.loads(encoded_Dat)
#
#     for event in safeget(json_Dat, "_embedded", "events"):
#         if name_ok(safeget(event, "name"), spotify_artist):
#             venue_dict = safeget(event, "_embedded", "venues", 0, "name")
#             city = safeget(event, "_embedded", "venues", 0, "city", "name")
#             event_state = safeget(event, "_embedded", "venues", 0, "state", "name")
#             date_UTC = (
#                 safeget(event, "dates", "start", "dateTime")
#                 .replace("Z", "")
#                 .replace("T", " ")
#             )
#             event_sale_start = safeget(event, "sales", "public", "startDateTime")
#             event_lowest_price = safeget(event, "priceRanges", 0, "min")
#             event_highest_price = safeget(event, "priceRanges", 0, "max")


def ticketmaster_pull_caller():
    """
    MAIN API FUNCTION

        Get top 250 artists from the SQL table with relevant artists (they actually have upcoming events on stubhub)

        Loop through these artists, making a request to the Ticketmaster API for each encoded artist string

        Only keep records where the event name has an adequate fuzzy match score to the artist name

    """
    artists_df = data_fetch_pymysql().head(ARTIST_COUNT)

    for artist_dat in artists_df.iterrows():
        spotify_artist = artist_dat[1]["artist"]
        spotify_artist_id = artist_dat[1]["artist_id"]
        artist_encode = spotify_artist.encode("utf-8")
        artist_decode = unidecode(str(artist_encode, encoding="utf-8"))
        artist_keyword = artist_decode.replace(" ", "+")

        pull_from_api(spotify_artist)


#pull_from_api("taylor+swift")
#ticketmaster_pull_caller()

logger.info("Program done")