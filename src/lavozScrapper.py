# -*- coding: utf-8 -*-

import datetime
from bs4 import BeautifulSoup
import pandas as pd
import os
import logging
import re
from progressBarPrinter import print_progress_bar
from utils import get_formated_telephone

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
fh = logging.FileHandler('logs/lavozScrapper.log')
fh.setLevel(logging.INFO)
formatter = logging.Formatter('[%(asctime)s][%(levelname)s] %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)


class LaVozScrapper:

    def __init__(self, config, request_getter, path):
        self.from_page = config["from_page"]
        self.pages = config["pages"]
        self.publisher_types = config["publisher_types"]
        self.ids_filename = config["ids_filename"]
        self.result_filename = config["result_filename"]
        self.request_getter = request_getter
        self.path = path
        self.ids_directory = self.path + "/ids_to_search/lavoz"
        logger.info("Start with configuration: [{}]".format(config))
        print("----------------------")
        print("LA VOZ SCRAPPER CONFIGURATION:")
        print("page from: {}".format(self.from_page))
        print("pages: {}".format(self.pages))
        print("publishers types: {}".format(self.publisher_types))
        print("id filename prefix: {}".format(self.ids_filename))
        print("result filename prefix: {}".format(self.result_filename))
        print("----------------------")

    def get(self, url):
        response = self.request_getter.get(url)
        return BeautifulSoup(response.content, 'html.parser') if response is not None else response

    def get_house_info(self, link):
        logger.info("Start get_house_info")
        response_house = self.get(link)
        info = {}
        if response_house:

            # Get general info
            for meta in response_house.find_all("meta"):
                name = meta.get("name")
                prop = meta.get("property")
                content = meta.get("content")
                if name is not None:
                    name = name.replace("cXenseParse:", "")
                    info[name] = content
                if prop is not None:
                    prop = prop.replace("og:", "")
                    info[prop] = content

            if "recs:articleid" in info:
                art_id = info["recs:articleid"]
            else:
                p = re.compile(r'/\d+/')
                art_id = p.search(link).group(0)[1:-1]

            tag_telefono = response_house.find(id="tel")
            info['telephone'] = tag_telefono.get_text() if tag_telefono else ""
            info['telephone_formated'] = get_formated_telephone(info['telephone'])

        logger.info("End get_house_info")
        return info

    def scrap_ids(self):
        logger.info("Start scrap_ids")
        print("Start LAVOZ ID Scrapping")
        for publisher_type in self.publisher_types:
            logger.info("Start scrap_ids {}".format(publisher_type))
            print("Start {} id scrapping".format(publisher_type))
            houses_urls_df = pd.DataFrame()
            print_progress_bar(0, self.from_page + self.pages - 1, publisher_type + " ids")
            for i in range(0, self.from_page + self.pages - 1):
                page_part = "&page=" + str(i)

                if i == 0:
                    page_part = ""

                search_url_inmu = 'https://clasificados.lavoz.com.ar/buscar/inmuebles?filters={{"vendedor":["{publisher_type}"]}}{page_part}'.format(
                    publisher_type=publisher_type, page_part=page_part).replace("\'", '"')
                response_soup = self.get(search_url_inmu)
                if not response_soup:
                    logger.info("Error trying to get this page: number[{}] publisher_type[{}] url[{}]".format(i + 1,
                                                                                                              publisher_type,
                                                                                                              search_url_inmu))
                    continue
                house_list = response_soup.find_all("a", {"class": "text-decoration-none"})
                if (not house_list) or len(house_list) <= 0:
                    logger.info(
                        "Error trying to get the houses list: number[{}] publisher_type[{}] url[{}]".format(i + 1,
                                                                                                            publisher_type,
                                                                                                            search_url_inmu))
                    continue

                house_urls = list(set(house_item.get("href") for house_item in house_list))

                house_processed = [False for house in house_urls]
                houses_info = pd.DataFrame({
                    "url": house_urls,
                    "processed": house_processed
                })
                houses_urls_df = pd.concat([houses_urls_df, houses_info], axis=0, sort=False).reset_index().drop(
                    columns="index")
                houses_urls_df.to_csv(self.path + "/temp/temp_{}_{}.csv".format(publisher_type, self.ids_filename),
                                      index=False,
                                      encoding="UTF-8")
                print_progress_bar(i + 1, self.from_page + self.pages - 1, publisher_type + " ids")

            if not os.path.exists(self.ids_directory):
                os.makedirs(self.ids_directory)

            houses_urls_df.to_csv(self.ids_directory + "/" + self.ids_filename + "_" + publisher_type + "_ids.csv",
                                  index=False,
                                  encoding="UTF-8")
            logger.info("End scrap_ids {}".format(publisher_type))
            print("")
            print("End {} id scrapping".format(publisher_type))
        logger.info("End scrap_ids")
        print("End LAVOZ ID Scrapping")

    def get_houses_info(self):
        logger.info("Start get_houses_info")
        print("Start LAVOZ houses info Scrapping")
        for publisher_type in self.publisher_types:
            logger.info("Start get_houses_info {}".format(publisher_type))
            print("Start {} houses info Scrapping".format(publisher_type))
            houses_df = pd.DataFrame()
            houses_urls_df = pd.read_csv(
                self.ids_directory + "/" + self.ids_filename + "_" + publisher_type + "_ids.csv")
            houses_urls_df.drop_duplicates(subset="url", keep="first", inplace=True)
            i = 0
            total_rows = houses_urls_df.shape[0]
            print_progress_bar(i, total_rows, publisher_type + " " + str(i))
            for row in houses_urls_df.itertuples():
                i += 1
                house_info = self.get_house_info(row.url)
                if house_info == {}:
                    logger.info("This house could not be processed: {}".format(row.url))
                    continue
                house_info_df = pd.DataFrame([house_info], columns=house_info.keys())
                houses_urls_df.loc[row.Index, 'processed'] = True
                houses_df = pd.concat([houses_df, house_info_df], axis=0, sort=False).reset_index().drop(
                    columns="index")
                if i % 5 == 0:
                    houses_df.to_csv(self.path + "/temp/temp_{}_{}.csv".format(self.result_filename, publisher_type),
                                     index=False, encoding="UTF-8")
                    houses_urls_df.to_csv(
                        self.path + "/temp/temp_{}_{}_ids.csv".format(self.result_filename, publisher_type),
                        index=False, encoding="UTF-8")
                print_progress_bar(i, total_rows, publisher_type + " " + str(i))

            directory = self.path + "/results/" + datetime.datetime.today().strftime('%Y-%m-%d')
            if not os.path.exists(directory):
                os.makedirs(directory)
            houses_df.to_csv(directory + "/{}_{}.csv".format(self.result_filename, publisher_type), index=False,
                             encoding="UTF-8")
            houses_urls_df.to_csv(self.ids_directory + "/" + self.ids_filename + "_" + publisher_type + "_ids.csv",
                                  index=False)
            logger.info("End get_houses_info {}".format(publisher_type))
            print("")
            print("End {} houses info Scrapping".format(publisher_type))
        logger.info("End get_houses_info")
        print("End LAVOZ houses info Scrapping")
