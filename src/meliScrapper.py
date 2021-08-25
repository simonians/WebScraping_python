import datetime
from bs4 import BeautifulSoup
import pandas as pd
import os
import logging
import re
from bs4 import Tag

from progressBarPrinter import print_progress_bar
from utils import get_formated_telephone

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
fh = logging.FileHandler('logs/meliScrapper.log')
fh.setLevel(logging.INFO)
formatter = logging.Formatter('[%(asctime)s][%(levelname)s] %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)


class MeliScrapper:
    def __init__(self, config, request_getter, path):
        self.from_page = config["from_page"]
        self.pages = config["pages"]
        self.publisher_types = config["publisher_types"]
        self.operation_types = config["operation_types"]
        self.ids_filename = config["ids_filename"]
        self.result_filename = config["result_filename"]
        self.request_getter = request_getter
        self.path = path
        self.ids_directory = self.path + "/ids_to_search/meli"

        print("----------------------")
        print("MELI SCRAPPER CONFIGURATION:")
        print("page from: {}".format(self.from_page))
        print("pages: {}".format(self.pages))
        print("publishers types: {}".format(self.publisher_types))
        print("id filename prefix: {}".format(self.ids_filename))
        print("result filename prefix: {}".format(self.result_filename))
        print("----------------------")

    def get(self, url):
        response = self.request_getter.get(url)
        return BeautifulSoup(response.content, 'html.parser') if response is not None else response

    def scrap_ids(self):
        logger.info("Start scrap_ids")
        print("Start Mercado libre scrapping")
        for publisher_type in self.publisher_types:
            for operation_type in self.operation_types:
                print("Start {}-{} id scrapping".format(publisher_type, operation_type))
                houses_urls_df = pd.DataFrame()
                print_progress_bar(0, self.from_page + self.pages, "{}-{} ids".format(publisher_type, operation_type))

                for i in range(self.from_page, self.from_page + self.pages):

                    page_part_number = 48 * (i - 1) + 1

                    page_part = "_Desde_" + str(page_part_number)

                    search_url_inmu = \
                        'https://inmuebles.mercadolibre.com.ar/{operation_type}/{publisher_type}/{page_part}'.format(
                            publisher_type=publisher_type, page_part=page_part, operation_type=operation_type)

                    response_soup = self.get(search_url_inmu)
                    house_list = response_soup.find_all("a", {"class": "item__info-link"})
                    house_urls = list(set(house_item.get("href") for house_item in house_list))
                    house_processed = [False for house in house_urls]
                    houses_info = pd.DataFrame({
                        "url": house_urls,
                        "processed": house_processed
                    })
                    houses_urls_df = pd.concat([houses_urls_df, houses_info], axis=0, sort=False).reset_index().drop(
                        columns="index")
                    houses_urls_df.to_csv(
                        self.path + "/temp/temp_{}_{}_{}.csv".format(publisher_type, operation_type, self.ids_filename),
                        index=False,
                        encoding="UTF-8")

                    print_progress_bar(i + 1, self.from_page + self.pages,
                                       "{}-{} ids".format(publisher_type, operation_type))
                    if not os.path.exists(self.ids_directory):
                        os.makedirs(self.ids_directory)

                    houses_urls_df.to_csv(
                        "{directory}/{name}_{publisher_type}_{operation_type}_ids.csv".format(
                            directory=self.ids_directory,
                            name=self.ids_filename,
                            publisher_type=publisher_type,
                            operation_type=operation_type
                        ),
                        index=False,
                        encoding="UTF-8")
                print("")
                print("End {}-{} id scrapping".format(publisher_type, operation_type))
        print("End Mercado Libre ID Scrapping")
        logger.info("End scrap_ids")

    def get_data(self, base, content_lists):
        index = content_lists.pop(0)
        try:
            if content_lists:
                data = self.get_data(base[index].contents, content_lists)
            else:
                data = base[index]
        except:
            return None

        return data

    def get_lat_long(self, response_house):
        lat_long = ' %2C '
        google_map_url = 'https://maps.googleapis.com/maps/api/staticmap?center='
        for script in response_house.find_all('script'):
            if google_map_url in script.get_text():
                lat_long = \
                    script.get_text().split(google_map_url)[1].split('&zoom')[0]

        return tuple(lat_long.split('%2C'))

    def get_house_info(self, link):
        logger.info("Start get_house_info")
        response_house = self.get(link)
        info = {}
        if response_house:
            short_description = response_house.find("section", {"class": "short-description--static"})

            info['shortDescription'] = self.get_data(short_description.find("h1").contents, [0]).strip()
            info['type'] = link[8:].split('.')[0]
            info['currency'] = self.get_data(short_description.find("span").contents, [1, 0])
            info['price'] = self.get_data(short_description.find("span").contents, [3, 0])
            info['size'] = self.get_data(short_description.find_all("dl"), [1, 2, 0])
            info['rooms'] = self.get_data(short_description.find_all("dl"), [2, 2, 0])
            info['bathrooms'] = self.get_data(short_description.find_all("dl"), [3, 2, 0])
            info['link'] = link

            section_view_more = response_house.find("section", {"class": "vip-section-seller-info"})

            info['name'] = self.get_data(section_view_more.contents, [5, 1, 0])

            for i, phone in enumerate(section_view_more.find_all("span", {"class": "profile-info-phone-value"})):
                info['phone_{}'.format(i)] = phone.get_text()
                info['phone_{}_formated'.format(i)] = get_formated_telephone(phone.get_text())

            info['address'] = self.get_data(response_house.find("h2", {"class": "map-address"}).contents, [0])
            info['location'] = self.get_data(response_house.find("h3", {"class": "map-location"}).contents, [0])

            try:
                info['description'] = response_house.find("div", {"class": "item-description__text"}).find(
                    "p").get_text().replace('<br>', '')
            except:
                info['description'] = ""

            info['latitude'], info['longitude'] = self.get_lat_long(response_house)

            spec_items = response_house.find("ul", {"class": "specs-list"})
            if spec_items:
                for spec_item in spec_items.children:
                    if isinstance(spec_item, Tag):
                        info[spec_item.find('strong').get_text().replace(' ', '_')] = spec_item.find('span').get_text()

        logger.info("End get_house_info")
        return info

    def houses_id_info(self):
        logger.info("Start houses_id_info")
        print("Start Mercado Libre houses info Scrapping")
        for publisher_type in self.publisher_types:
            for operation_type in self.operation_types:
                print("Start {}-{} houses info Scrapping".format(publisher_type, operation_type))
                houses_df = pd.DataFrame()
                houses_urls_df = pd.read_csv("{directory}/{name}_{publisher_type}_{operation_type}_ids.csv".format(
                    directory=self.ids_directory,
                    name=self.ids_filename,
                    publisher_type=publisher_type,
                    operation_type=operation_type
                ))
                houses_urls_df.drop_duplicates(subset="url", keep="first", inplace=True)
                i = 0
                total_rows = houses_urls_df.shape[0]
                print_progress_bar(i, total_rows, publisher_type + " " + str(i))
                for row in houses_urls_df.itertuples():
                    i += 1
                    house_info = self.get_house_info(row.url)
                    house_info_df = pd.DataFrame([house_info], columns=house_info.keys())
                    houses_urls_df.loc[row.Index, 'processed'] = True
                    houses_df = pd.concat([houses_df, house_info_df], axis=0, sort=False).reset_index().drop(
                        columns="index")
                    if i % 5 == 0:
                        houses_df.to_csv(
                            self.path + "/temp/temp_{}_{}_{}.csv".format(self.result_filename, publisher_type,
                                                                         operation_type),
                            index=False, encoding="UTF-8")
                        houses_urls_df.to_csv(
                            self.path + "/temp/temp_{}_{}_{}_ids.csv".format(self.result_filename, publisher_type,
                                                                             operation_type),
                            index=False, encoding="UTF-8")
                    print_progress_bar(i, total_rows, publisher_type + "-" + operation_type + " " + str(i))

                directory = self.path + "/results/" + datetime.datetime.today().strftime('%Y-%m-%d')
                if not os.path.exists(directory):
                    os.makedirs(directory)
                houses_df.to_csv(
                    directory + "/{}_{}_{}.csv".format(self.result_filename, publisher_type, operation_type),
                    index=False,
                    encoding="UTF-8")
                houses_urls_df.to_csv("{directory}/{name}_{publisher_type}_{operation_type}_ids.csv".format(
                    directory=self.ids_directory,
                    name=self.ids_filename,
                    publisher_type=publisher_type,
                    operation_type=operation_type),
                    index=False)
                print("")
                print("End {} houses info Scrapping".format(publisher_type))

        logger.info("End houses_id_info")
        print("End Mercado Libre houses info Scrapping")
