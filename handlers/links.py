from handlers.proxies import Proxies
from datetime import date

class Links:
    def __init__(self, main_url, yr_diff=20):
        self.main_url = main_url
        self.yr_diff = yr_diff
        self.proxy_obj = Proxies()

    def _get_all_seasons_links(self):
        def make_url_with_year(year):
            return f"{self.main_url}{year}"

        today_yr = date.today().year
        historic_yr = today_yr - self.yr_diff
        return [make_url_with_year(year) for year in range(historic_yr, today_yr)]