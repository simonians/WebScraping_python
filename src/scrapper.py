# -*- coding: utf-8 -*-


import os
import json
import logging

from meliScrapper import MeliScrapper
from requestgetter import RequestGetter
from lavozScrapper import LaVozScrapper
from zonapropScrapper import ZonapropScrapper
from random import randint

path = os.path.dirname(os.path.realpath('__file__'))
with open(path + "/config.json") as data_file:
    config = json.load(data_file)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
fh = logging.FileHandler('logs/scrapper.log')
fh.setLevel(logging.INFO)
formatter = logging.Formatter('[%(asctime)s][%(levelname)s] %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)

logger.info("-----Start Proppi Scrapper----")
print("-----Start Proppi Scrapper----")

scrap_lavoz = config['scrap_lavoz'].lower() == "true"
scrap_meli = config['scrap_meli'].lower() == "true"
scrap_zonaprop = config['scrap_zonaprop'].lower() == "true"

config["zonaprop"]["sleep"] = config['requests']['use_proxy'] == "False"

request_getter = RequestGetter(config['requests'])

if scrap_lavoz:
    lavoz_scrapper = LaVozScrapper(config["lavoz"], request_getter, path)
    lavoz_scrapper.scrap_ids()
    lavoz_scrapper.get_houses_info()

if scrap_meli:
    meli_scrapper = MeliScrapper(config["meli"], request_getter, path)
    meli_scrapper.scrap_ids()
    meli_scrapper.houses_id_info()

if scrap_zonaprop:
    zonaprop_scrapper = ZonapropScrapper(config["zonaprop"], request_getter, path)
    zonaprop_scrapper.scrap()

phrases = ["The best way to predict the future is to create it.",
           "Live as if you were to die tomorrow.Learn as if you were to live forever.",
           "Do the difficult things while they are easy and do the great things while they are small. A journey of a thousand miles begins with a single step.",
           "Today a reader, tomorrow a leader,",
           "If you can dream it, you can do it.",
           "Ever tried. Ever failed. No matter. Try again. Fail again. Fail better.",
           "Tell me and I forget. Teach me and I remember. Involve me and I learn"]

logger.info("-----End Proppi Scrapper----")
print("-----End Proppi Scrapper Succsesfully----")
print("I hope you have a really nice day, and remember:")
print(phrases[randint(0, len(phrases) - 1)])

