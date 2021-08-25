# -*- coding: utf-8 -*-

from bs4 import BeautifulSoup
import pandas as pd
import datetime
import os
import json
import logging

from progressBarPrinter import print_progress_bar

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
fh = logging.FileHandler('logs/olxScrapper.log')
fh.setLevel(logging.INFO)
formatter = logging.Formatter('[%(asctime)s][%(levelname)s] %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)


class OlxScrapper:
    def __init__(self, config, request_getter, path):
        self.from_page = config["from_page"]
        self.pages = config["pages"]
        self.provinces = config["provinces"]
        self.ids_filename = config["ids_filename"]
        self.result_filename = config["result_filename"]
        self.request_getter = request_getter
        self.path = path
        self.ids_directory = self.path + "/ids_to_search/olx"
        logger.info("Start with configuration: [{}]".format(config))
        print("----------------------")
        print("OLX SCRAPPER CONFIGURATION:")
        print("page from: {}".format(self.from_page))
        print("pages: {}".format(self.pages))
        print("provinces: {}".format(self.provinces))
        print("id filename prefix: {}".format(self.ids_filename))
        print("result filename prefix: {}".format(self.result_filename))
        print("----------------------")

    def get_location_info(self, dict):
        name = dict["name"]
        if "children" in dict and len(dict["children"]) > 0:
            return name + " - " + self.get_location_info(dict["children"][0])
        else:
            return name

    def get_optionals(self, optionals, info):
        for optional in optionals:
            info[optional["name"]] = optional["value"]

    def get_house_info(self, id):
        logger.info("Start get_house_info id:[{}]".format(id))
        link = "https://www.olx.com.ar/api-v2/items/{}".format(id)
        response = self.request_getter.get(link)
        if response.status_code == 403:
            return {}
        response_dict = json.loads(response.content)
        info = {}

        # Coordinates
        if "coordinates" in response_dict and response_dict["coordinates"] is not None:
            info["latitude"] = response_dict["coordinates"]["latitude"] if "latitude" in response_dict[
                "coordinates"] else ""
            info["longitude"] = response_dict["coordinates"]["longitude"] if "longitude" in response_dict[
                "coordinates"] else ""
        # Price
        info["priceType"] = response_dict["priceType"] if "priceType" in response_dict else ""
        if "price" in response_dict and response_dict["price"] is not None:
            info["amount"] = response_dict["price"]["amount"] if "amount" in response_dict["price"] else ""
            info["preCurrency"] = response_dict["price"]["preCurrency"] if "preCurrency" in response_dict[
                "price"] else ""
            info["displayPrice"] = response_dict["price"]["displayPrice"] if "displayPrice" in response_dict[
                "price"] else ""

        # GeneralInfo
        info["description"] = response_dict["description"] if "description" in response_dict else ""
        info["title"] = response_dict["title"] if "title" in response_dict else ""

        # Contacto
        info["contactName"] = response_dict["contactName"] if "contactName" in response_dict else ""
        info["link"] = response_dict["slug"] if "slug" in response_dict else ""
        info["phoneType"] = response_dict["phoneType"] if "phoneType" in response_dict else ""
        info["phone"] = response_dict["phone"] if "phone" in response_dict else ""
        info["location"] = self.get_location_info(response_dict["location"]) if "location" in response_dict else ""
        if "user" in response_dict and response_dict["user"] is not None:
            info["professional"] = response_dict["user"]["professional"] if "professional" in response_dict[
                "user"] else ""
            info["publicName"] = response_dict["user"]["publicName"] if "publicName" in response_dict["user"] else ""
            info["userName"] = response_dict["user"]["userName"] if "userName" in response_dict["user"] else ""

        # date
        if "date" in response_dict and response_dict["date"] is not None:
            info["date"] = response_dict["date"]["timestamp"] if "timestamp" in response_dict["date"] else ""

        # Category
        if "category" in response_dict and response_dict["category"] is not None:
            info["category"] = response_dict["category"]["originalName"] if "originalName" in response_dict[
                "category"] else ""

        # Get Optionals
        self.get_optionals(response_dict["optionals"], info)
        logger.info("End get_house_info id:[{}]".format(id))
        return info

    def scrap_ids(self):
        logger.info("Start scrap_ids")
        print("Start OLX ID Scrapping")
        for province in self.provinces:
            logger.info("Start scrap_ids {}".format(province))
            print("Start {} ID Scrapping".format(province))
            house_ids_df = pd.DataFrame()
            ids_list = []
            print_progress_bar(0, self.from_page + self.pages - 1, province + " ids")
            for i in range(self.from_page - 1, self.from_page + self.pages - 1):
                page_part = "-p-" + str(i + 1)
                if i == 0:
                    page_part = ""

                search_url = "https://{}.olx.com.ar/inmuebles-y-propiedades-cat-16{}".format(province, page_part)
                response = self.request_getter.get(search_url)
                response_soup = BeautifulSoup(response.content, 'html.parser') if response is not None else response
                if response_soup:
                    pages_id_list = response_soup.find("div", {"id": "tracking-data"})

                if pages_id_list is None:
                    continue

                json_id_list = json.loads(pages_id_list.get_text())
                ids_list = ids_list + json_id_list['mixpanel']['props']['extra']
                house_ids_df = pd.DataFrame({
                    'id': ids_list,
                    'processed': [False for id in ids_list]})
                house_ids_df.to_csv(self.path + "/temp/temp_{}_{}.csv".format(province, self.ids_filename),
                                    index=False,
                                    encoding="UTF-8")
                print_progress_bar(i + 1, self.from_page + self.pages - 1, province + " ids")
            if not os.path.exists(self.ids_directory):
                os.makedirs(self.ids_directory)

            house_ids_df.to_csv(self.ids_directory + "/" + self.ids_filename + "_" + province + "_ids.csv",
                                index=False,
                                encoding="UTF-8")
            logger.info("End scrap_ids {}".format(province))
            print("")
            print("End {} ID Scrapping".format(province))
        logger.info("End scrap_ids")
        print("End OLX ID Scrapping")

    def get_houses_info(self):
        logger.info("Start get_houses_info")
        print("Start OLX houses info Scrapping")
        for province in self.provinces:
            logger.info("Start get_houses_info {}".format(province))
            print("Start {} houses info Scrapping".format(province))
            houses_ids_df = pd.read_csv(
                self.ids_directory + "/" + self.ids_filename + "_" + province + "_ids.csv")
            houses_ids_df.drop_duplicates(subset="id", keep="first", inplace=True)

            to_proceess = houses_ids_df[~houses_ids_df.processed]

            houses_df = pd.DataFrame()
            i = 0
            total_rows = houses_ids_df.shape[0]
            print_progress_bar(i, total_rows, province + " " + str(i))
            for row in to_proceess.itertuples():
                i += 1
                house_info = self.get_house_info(row.id)
                if house_info == {}:
                    logger.info("This house could not be processed: {}".format(row.id))
                    continue
                house_info_df = pd.DataFrame([house_info], columns=house_info.keys())
                houses_df = pd.concat([houses_df, house_info_df], axis=0, sort=False).reset_index().drop(
                    columns="index")
                houses_ids_df.loc[row.Index, 'processed'] = True
                if i % 5 == 0:
                    houses_df.to_csv(self.path + "/temp/temp_{}_{}.csv".format(self.result_filename, province),
                                     index=False, encoding="UTF-8")
                    houses_ids_df.to_csv(
                        self.path + "/temp/temp_{}_{}_ids.csv".format(self.result_filename, province),
                        index=False, encoding="UTF-8")
                print_progress_bar(i, total_rows, province + " " + str(i))

            directory = self.path + "/results/" + datetime.datetime.today().strftime('%Y-%m-%d')
            if not os.path.exists(directory):
                os.makedirs(directory)
            houses_df.to_csv(directory + "/{}_{}.csv".format(self.result_filename, province), index=False,
                             encoding="UTF-8")
            houses_ids_df.to_csv(self.ids_directory + "/" + self.ids_filename + "_" + province + "_ids.csv",
                                 index=False)
            logger.info("End get_houses_info {}".format(province))
            print("")
            print("End {} houses info Scrapping".format(province))
        logger.info("End get_houses_info")
        print("End OLX houses info Scrapping")
