from time import sleep
import requests
from lxml.html import fromstring
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
fh = logging.FileHandler('logs/requestgetter.log')
fh.setLevel(logging.INFO)
formatter = logging.Formatter('[%(asctime)s][%(levelname)s] %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)


class RequestGetter:
    def __init__(self, config):
        self.proxy_list = []
        self.current_proxy = []
        self.proxy_list = []
        self.use_proxy = config['use_proxy'].lower() == "true"
        self.max_attempts = config["max_attempts"]
        self.sleep_time = config["sleep_time"]
        logger.info("Start with configuration: [{}]".format(config))
        print("----------------------")
        print("REQUEST CONFIGURATION:")
        print("use proxy: {}".format(self.use_proxy))
        print("max attempts: {}".format(self.max_attempts))
        print("sleep time: {}".format(self.sleep_time))
        print("----------------------")

    def get_proxies(self):
        logger.info("Start get_proxies")
        url = 'https://free-proxy-list.net/'
        response = requests.get(url)
        parser = fromstring(response.text)
        proxies = []
        for i in parser.xpath('//tbody/tr')[:10]:
            if i.xpath('.//td[7][contains(text(),"yes")]'):
                proxy = ":".join([i.xpath('.//td[1]/text()')[0], i.xpath('.//td[2]/text()')[0]])
                proxies.append(proxy)
        self.proxy_list = proxies
        logger.info("End get_proxies :[{}]".format(proxies))

    def get_proxy(self):
        if len(self.proxy_list) == 0:
            self.get_proxies()
        return self.proxy_list.pop()

    def update_current_proxy(self):
        logger.info("Start update_current_proxy")
        if not self.current_proxy:
            self.current_proxy = self.get_proxy()
        logger.info("End update_current_proxy:[{}]".format(self.current_proxy))

    def delete_current_proxy(self):
        logger.info("Start delete_current_proxy")
        self.current_proxy = None
        logger.info("End delete_current_proxy")

    def get_without_proxy(self, url):
        logger.info("Start get_without_proxy")
        response = None
        try:
            response = requests.get(url)
            sleep(self.sleep_time)
            logger.info("End get_without_proxy OK:[{}]".format(response))
        except Exception as e:
            logger.error("This url has not been processed: {} | Exception: {}".format(url, e))

        return response

    def get_with_proxy(self, url):
        logger.info("Start get_with_proxy url:[{}]".format(url))
        attempts = 0
        while attempts < self.max_attempts:
            try:
                self.update_current_proxy()
                response = requests.get(url, proxies={"http": self.current_proxy, "https": self.current_proxy},
                                        timeout=10)
                logger.info(
                    "End get_with_proxy proxy:[{}], response[{}],url:[{}]".format(self.current_proxy, response, url))
                return response
            except Exception as e:
                attempts += 1
                self.delete_current_proxy()
                logger.info(
                    "Error get_with_proxy number:[{}], proxy:[{}],url:[{}]".format(attempts, self.current_proxy, url))
        logger.info("get_with_proxy max attempts reached, trying without proxy")
        return self.get_without_proxy(url)

    def get(self, url, skip_proxy=False):
        return self.get_with_proxy(url) if self.use_proxy and not skip_proxy else self.get_without_proxy(url)

    def post(self, url, data, headers=None):
        return self.post_with_proxy(url, data, headers) if self.use_proxy else self.post_without_proxy(url, data,
                                                                                                       headers)

    def post_without_proxy(self, url, data, headers=None):
        logger.info("Start post_without_proxy")
        response = None
        try:
            response = requests.post(url,
                                     data=data,
                                     headers=headers,
                                     timeout=10)
            sleep(self.sleep_time)
            logger.info(
                "End post_without_proxy, response[{response}], url:[{url}],"
                "data:[{data}], headers:[{headers}]".format(response=response,
                                                            url=url,
                                                            data=data,
                                                            headers=headers))
        except Exception as e:
            logger.error("This url has not been processed: {} | Exception: {}".format(url, e))

        return response

    def post_with_proxy(self, url, data, headers=None):
        logger.info("Start post_with_proxy url:[{}]".format(url))
        attempts = 0
        while attempts < self.max_attempts:
            try:
                self.update_current_proxy()
                response = requests.post(url,
                                         data=data,
                                         headers=headers,
                                         proxies={"http": self.current_proxy, "https": self.current_proxy},
                                         timeout=10)
                logger.info(
                    "End post_with_proxy proxy:[{proxy}], response[{response}], url:[{url}]," 
                    "data:[{data}], headers:[{headers}]".format(proxy=self.current_proxy,
                                                                response=response,
                                                                url=url,
                                                                data=data,
                                                                headers=headers))
                if not response.ok:
                    raise Exception("Not 200 Exception")
                return response
            except Exception as e:
                attempts += 1
                logger.info(
                    "Error post_with_proxy number:[{}], proxy:[{}],url:[{}]".format(attempts, self.current_proxy, url))
                self.delete_current_proxy()
        logger.info("post_with_proxy max attempts reached, trying without proxy")
        return self.post_without_proxy(url, data, headers)
