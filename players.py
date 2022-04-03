from handlers.proxies import Proxies
from handlers.data_request import DataRequest
from handlers.df_html_reader_validator import df_html_validator
from datetime import date
import pandas as pd

class PlayersLinks:
    def __init__(self, main_url, yr_diff = 20):
        self.main_url = main_url
        self.yr_diff = yr_diff
        self.proxy_obj = Proxies()

    def _get_players_all_seasons_links(self):

        def make_url_with_year(year):
            return f"{self.main_url}{year}"

        today_yr = date.today().year
        historic_yr = today_yr - self.yr_diff
        return [make_url_with_year(year) for year in range(historic_yr,today_yr)]

    @df_html_validator
    def _one_season_data(self, players_links_one_season):
        data_request_object = DataRequest(players_links_one_season, self.proxy_obj.header, self.proxy_obj.get_new_ip())
        df = pd.concat(data_request_object.get_df).reset_index(drop = True)
        players_soup_arr = data_request_object.get_soup.find_all("a", {"class": "list-player-entry"})
        players_dict = {suop_obj["title"] : f'https://www.proballers.com{suop_obj["href"]}'\
                                               for suop_obj in players_soup_arr}
        links_df = pd.DataFrame.from_dict(players_dict, orient= 'index', columns = ['link'])
        return df.join(links_df, on = ['Basketball Player'])

    def all_seasons_data(self):
        links_list = self._get_players_all_seasons_links()
        dfs_list = [self._one_season_data(link) for link in links_list]
        return pd.concat(dfs_list).reset_index(drop = True)

main_url = 'https://www.proballers.com/basketball/league/50/greece-heba-a1/players/'
PlayersLinks(main_url).all_seasons_data()
