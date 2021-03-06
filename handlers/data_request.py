import requests
from bs4 import BeautifulSoup
import pandas as pd
from handlers.delay import normal_delay
from handlers.df_html_reader_validator import df_html_validator


class DataRequest:
    def __init__(self, url, header, proxy_ip):
        self.url = url
        self.header = header
        self.proxy_ip = proxy_ip
        self.raw_data = self.make_request()

    @normal_delay
    def make_request(self):
        return requests.get(self.url, headers=self.header, proxies={"http": self.proxy_ip})

    @property
    def get_soup(self):
        return BeautifulSoup(self.raw_data.text, 'html.parser')

    @property
    @df_html_validator
    def get_df(self):
        return pd.read_html(self.raw_data.text)
