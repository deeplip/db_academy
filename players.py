from handlers.proxies import Proxies
from handlers.data_request import DataRequest
from datetime import date
import pandas as pd
from tqdm import tqdm


class PlayersLinks:
    def __init__(self, main_url, yr_diff=20):
        self.main_url = main_url
        self.yr_diff = yr_diff
        self.proxy_obj = Proxies()

    def _get_players_all_seasons_links(self):
        def make_url_with_year(year):
            return f"{self.main_url}{year}"

        today_yr = date.today().year
        historic_yr = today_yr - self.yr_diff
        return [make_url_with_year(year) for year in range(historic_yr, today_yr)]

    def _one_season_data(self, players_links_one_season):
        data_request_object = DataRequest(players_links_one_season, self.proxy_obj.header, self.proxy_obj.get_new_ip())
        df = pd.concat(data_request_object.get_df).reset_index(drop=True)
        players_soup_arr = data_request_object.get_soup.find_all("a", {"class": "list-player-entry"})
        players_dict = {soup_obj["title"]: f'https://www.proballers.com{soup_obj["href"]}' \
                        for soup_obj in players_soup_arr}
        links_df = pd.DataFrame.from_dict(players_dict, orient='index', columns=['link'])
        return df.join(links_df, on=['Basketball Player'])

    def all_seasons_data(self):
        links_list = self._get_players_all_seasons_links()
        dfs_list = []
        for link in tqdm(links_list):
            dfs_list.append(self._one_season_data(link))
        return pd.concat(dfs_list).drop_duplicates().reset_index(drop=True)


class Player:
    def __init__(self, name, home_country, link):
        self.name = name
        self.link = link
        self.home_country = home_country
        self._raw_data = self._get_raw_data()
        self._dfs_list = self._raw_data.get_df
        self._soup_data = self._raw_data.get_soup
        self.regular_seasons_df = self._dfs_list[1]
        self._identity_list = self._identity_list()
        self.birthdate = self._identity_list[0]
        self.height = self._identity_list[2]
        self.position = self._identity_list[3]
        self.all_data_dfs_arr = self.regular_seasons_df.copy()

    def _get_raw_data(self):
        proxy_obj = Proxies()
        header = proxy_obj.header
        new_ip = proxy_obj.get_new_ip()
        return DataRequest(self.link, header, new_ip)

    def _identity_list(self):
        identity = self._soup_data.find_all("ul", {"class": "identity__profil"})[0].find_all("li")
        return [i.decode_contents() for i in identity]


class PlayersData:

    def __init__(self, league, data_file):
        self.league = league
        self.data = pd.read_csv(data_file)
        self.players = self.collect_players()

    def collect_players(self):
        players = []
        print(f'Iterations num: {self.data.shape[0]}')
        for row in tqdm(self.data.iloc):
            single_player_name = row['Basketball Player']
            single_player_hcountry = row['Home country']
            single_player_link = row['link']
            single_player = Player(single_player_name, single_player_hcountry, single_player_link)
            players.append(single_player)
        return players

    def collect_data_per_player(self):
        for player in self.players:
            player.all_data_dfs_arr['link'] = player.link
            player.all_data_dfs_arr['name'] = player.name
            player.all_data_dfs_arr['h_country'] = player.home_country
            player.all_data_dfs_arr['birthdate'] = player.birthdate
            player.all_data_dfs_arr['height'] = player.height
            player.all_data_dfs_arr['position'] = player.position

    def get_all_data_list(self):
        self.collect_data_per_player()
        return [player.all_data_dfs_arr for player in self.players]

    def concat_data(self):
        concat = pd.concat(self.get_all_data_list())
        concat.columns = [col.lower().replace(' ', '_') for col in concat.columns]
        return concat.reset_index(drop=True)

    def get_and_save(self):
        df = self.concat_data()
        to_del = ['fgm', 'fga', '3m', '3a', '1m', '1a']
        df = df.drop(to_del, axis=1, errors='ignore')
        df.to_csv(f'data/players_data_{self.league}.csv', index=False)