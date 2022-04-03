import requests
import numpy as np
import pandas as pd
from delay import normal_delay

class Proxies:
    url = "https://free-proxy-list.net/"
    header = {
      "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
      "X-Requested-With": "XMLHttpRequest"
    }

    def __init__(self):
        self.prox_req = self.reset_ip_list()

    @normal_delay
    def reset_ip_list(self):
        return requests.get(Proxies.url, headers=Proxies.header)

    def _list(self):
        ips_df =  pd.read_html(self.prox_req.text)[0][['IP Address', 'Port']]
        return ips_df['IP Address'] + ':' + ips_df['Port'].astype('str')

    def get_new_ip(self):
        return np.random.choice(self._list())

print(Proxies().get_new_ip())