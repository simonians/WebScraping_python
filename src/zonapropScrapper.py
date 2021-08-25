import json
import logging
from time import sleep
import random
import datetime

import pandas as pd
import os
from progressBarPrinter import print_progress_bar
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
fh = logging.FileHandler('logs/zonapropScrapper.log')
fh.setLevel(logging.INFO)
formatter = logging.Formatter('[%(asctime)s][%(levelname)s] %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)


class ZonapropScrapper:

    def __init__(self, config, request_getter, path):
        self.from_page = config["from_page"]
        self.pages = config["pages"]
        self.publisher_types = config["publisher_types"]
        self.ids_filename = config["ids_filename"]
        self.result_filename = config["result_filename"]
        self.sleep = config["sleep"]
        self.request_getter = request_getter
        self.path = path
        logger.info("Start with configuration: [{}]".format(config))
        print("----------------------")
        print("ZONA PROP SCRAPPER CONFIGURATION:")
        print("page from: {}".format(self.from_page))
        print("pages: {}".format(self.pages))
        print("publishers types: {}".format(self.publisher_types))
        print("id filename prefix: {}".format(self.ids_filename))
        print("result filename prefix: {}".format(self.result_filename))
        print("----------------------")

    def get(self, url):
        response = self.request_getter.get(url, skip_proxy=True)
        return BeautifulSoup(response.content, 'html.parser') if response is not None else response

    def post(self, url, data):
        headers = {
            'Content-Type': "application/x-www-form-urlencoded",
            'User-Agent': "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:72.0) Gecko/20100101 Firefox/72.0",
            'Host': "www.zonaprop.com.ar",
            'origin': "www.zonaprop.com.ar",
        }
        response = self.request_getter.post(url, data, headers)
        return response.json()

    def scrap(self):
        logger.info("Start scrap")
        print("Start ZonaProp Scrapping")
        for publisher_type in self.publisher_types:
            logger.info("Start {} scrapping".format(publisher_type))
            print("Start {} scrapping".format(publisher_type))
            houses_df = pd.DataFrame()
            print_progress_bar(0, (self.from_page + self.pages - 1) * 20, publisher_type + " houses")
            for i in range(self.from_page, self.from_page + self.pages):
                page_part = "-pagina-" + str(i) if i > 1 else ""
                search_url_inmu = 'https://www.zonaprop.com.ar/inmuebles-{publisher_type}{page_part}.html'.format(
                    publisher_type=publisher_type, page_part=page_part).replace("\'", '"')
                response_soup = self.get(search_url_inmu)
                for script in response_soup.find_all("script"):
                    if 'listPostings = ' in script.text:
                        house_list = script.text.split('listPostings = ')[1].split('const developmentData ')[0].strip()[:-1]
                        break

                houses_list_json = json.loads(house_list)
                processed_list = []
                for house_json, j in zip(houses_list_json, range(0, len(houses_list_json))):
                    house_info = self.process_house_info(house_json)
                    house_info.update(self.get_contact_info(house_json))
                    processed_list.append(house_info)
                    if self.sleep:
                        sleep_time = random.randint(15, 45)
                        print_progress_bar((i - 1) * 20 + j + 1, (self.from_page + self.pages - 1) * 20,
                                           publisher_type + " houses sleep " + str(sleep_time) + "seconds")
                        sleep(sleep_time)
                    else:
                        print_progress_bar((i - 1) * 20 + j + 1, (self.from_page + self.pages - 1) * 20,
                                           publisher_type + " houses")
                houses_temp_df = pd.DataFrame.from_records(processed_list)
                houses_df = pd.concat([houses_df, houses_temp_df], axis=0, sort=False).reset_index().drop(
                    columns="index")
                houses_df.to_csv(self.path + "/temp/temp_{}_{}.csv".format(self.result_filename, publisher_type),
                                 index=False, encoding="UTF-8")

            directory = self.path + "/results/" + datetime.datetime.today().strftime('%Y-%m-%d')
            if not os.path.exists(directory):
                os.makedirs(directory)
            houses_df.to_csv(directory + "/{}_{}.csv".format(self.result_filename, publisher_type), index=False,
                             encoding="UTF-8")
            logger.info("End scrap {}".format(publisher_type))
            print("")
            print("End {}  Scrapping".format(publisher_type))
        logger.info("End scrap")

    def process_house_info(self, house_json):
        logger.info("Start process_house_info")
        keys_to_keep = ["postingId", 'title', 'descriptionNormalized', "antiquity", 'url', 'postingType']
        house_info = {key: house_json[key] for key in keys_to_keep if key in house_json}
        house_info.update(self.get_operation_and_price(house_json))
        house_info.update(self.process_expenses(house_json))
        house_info.update(self.process_main_features(house_json))
        house_info.update(self.process_general_features(house_json))
        house_info.update(self.process_flag_features(house_json))
        house_info.update(self.process_real_state_type_and_subtype(house_json))
        house_info.update(self.process_pub_date(house_json))
        house_info.update(self.process_location(house_json))
        logger.info("End process_house_info")
        return house_info

    def get_operation_and_price(self, house_json):
        sub_info = house_json["priceOperationTypes"][0]
        operation_type = sub_info["operationType"]["name"]
        price = sub_info["prices"][0]["amount"]
        currency = sub_info["prices"][0]["currency"]
        return {
            "operation_type": operation_type,
            "price": price,
            "price_currency": currency
        }

    def process_expenses(self, house_json):
        expenses_json = {}
        if "expenses" in house_json and house_json['expenses']:
            expenses_info = house_json['expenses']
            expenses_json['expenses_amount'] = expenses_info['amount']
            expenses_json['expenses_currency'] = expenses_info['currency']
        return expenses_json

    def process_main_features(self, house_json):
        processed_main_features = {}
        main_features = house_json['mainFeatures']
        for key in main_features:
            feature_id = main_features[key]["label"].lower().replace(" ", "_")
            value = main_features[key]["value"]
            measure = main_features[key]["measure"]
            measure = measure if measure else ""
            processed_main_features[feature_id] = "{value} {measure}".format(value=value, measure=measure)
        return processed_main_features

    def process_general_features(self, house_json):
        processed_general_features = {}
        general_features = house_json['generalFeatures']
        for key in general_features:
            subfeature = general_features[key]
            for subkey in subfeature:
                feature_id = subfeature[subkey]["label"].lower().replace(" ", "_")
                processed_general_features[feature_id] = True
        return processed_general_features

    def process_flag_features(self, house_json):
        return {"publisher_type": house_json['flagsFeatures'][0]["featureId"]}

    def process_real_state_type_and_subtype(self, house_json):
        real_estate = {'house_type': house_json["realEstateType"]["name"]}
        if "realEstateSubtype" in house_json and house_json["realEstateSubtype"]:
            real_estate['house_subtype'] = house_json["realEstateSubtype"]["name"]
        return real_estate

    def process_pub_date(self, house_json):
        publication_date = house_json["publication"]["beginDate"]
        return {"publication_date": "{day}/{month}/{year}".format(
            year=publication_date["yearOfEra"],
            month=publication_date["monthOfYear"],
            day=publication_date["dayOfMonth"]
        )}

    def process_location(self, house_json):
        geolocation = {}
        if "postingLocation" in house_json and house_json["postingLocation"]:
            location_info = house_json["postingLocation"]
            if "address" in location_info and location_info["address"]:
                geolocation["address"] = location_info["address"]["name"]
            geolocation["city"] = location_info["location"]["name"]
            if "postingGeolocation" in location_info and location_info["postingGeolocation"]:
                geolocation['latitude'] = location_info["postingGeolocation"]["geolocation"]['latitude']
                geolocation['longitude'] = location_info["postingGeolocation"]["geolocation"]['longitude']
                geolocation["googlemaps_url"] = location_info["postingGeolocation"]["urlStaticMap"]
        return geolocation

    def valid_contact_response(self, contact_info):
        return contact_info["contenido"]["resultadoContacto"]['response']['codigo'] == 202

    def process_contact_info(self, contact_info):
        logger.info("Start process_contact_info")
        processed_contact_info = {}
        if "status" in contact_info and contact_info["status"]["status"] == 200 and self.valid_contact_response(
                contact_info):
            if "contenido" in contact_info and contact_info["contenido"]:
                content = contact_info["contenido"]
                if "anunciante" in content and content["anunciante"]:
                    processed_contact_info = {"contact_{}".format(key): content["anunciante"][key] for key in
                                              content["anunciante"]}
                if "resultadoContacto" in content and content["resultadoContacto"]:
                    contact_result = content["resultadoContacto"]
                    if "idUsuario" in contact_result and contact_result["idUsuario"]:
                        processed_contact_info["user_id"] = contact_result["idUsuario"]

        else:
            processed_contact_info["contact_error"] = contact_info["errorMessage"] + \
                                                      contact_info["contenido"]["resultadoContacto"]['response'][
                                                          'clave']
        logger.info("Start process_contact_info")
        return processed_contact_info

    def get_contact_info(self, house_info):
        data = "idAviso={}&page=ficha".format(house_info['postingId'])
        contact_response = self.post('https://www.zonaprop.com.ar/aviso_verDatosAnunciante.ajax', data)
        return self.process_contact_info(contact_response)

