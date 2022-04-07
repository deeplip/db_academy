from handlers.links import Links
from handlers.data_request import DataRequest
import pandas as pd
from tqdm import tqdm
from handlers.proxies import Proxies


class GamesLinks(Links):
    def __init__(self, main_url):
        super().__init__(main_url)

    def _one_page_one_season_data(self, data_request_object):
        soup = data_request_object.get_soup

        def extract_dates():
            dates_objects = soup.find_all("h2", {"class": "generic-section-subtitle"})
            return [date.decode_contents().strip() for date in dates_objects]

        dates_list = extract_dates()
        dfs_list = data_request_object.get_df

        for num, date in enumerate(dates_list):
            dfs_list[num]['date'] = date
        df = pd.concat(dfs_list).reset_index(drop=True)

        def get_links():
            all_tr = soup.find_all("tr")
            url_part = "https://www.proballers.com"
            links = []
            for tr in range(1, len(all_tr)):
                try:
                    links.append(url_part + all_tr[tr].find_all("td")[-1].a["href"])
                except:
                    pass
            return links

        try:
            df = df.dropna()
            df['link'] = get_links()
        except:
            df['link'] = ''

        return df

    def get(self):

        def get_pages(data_request_object):
            soup = data_request_object.get_soup
            return len(soup.find_all("a", {"class": "pagination-link"}))

        links = self._get_all_seasons_links()
        dfs_list = []
        header = self.proxy_obj.header
        for link in tqdm(links):
            data_request_object = DataRequest(link, header, self.proxy_obj.get_new_ip())
            page_one_dfs = self._one_page_one_season_data(data_request_object)
            page_one_dfs['season'] = link[-4:]
            dfs_list.append(page_one_dfs)
            pages = get_pages(data_request_object)

            for page in range(pages):
                data_request_object = DataRequest(link + f'/{page}', header, self.proxy_obj.get_new_ip())
                temp_df = self._one_page_one_season_data(data_request_object)
                temp_df['season'] = link[-4:]
                dfs_list.append(temp_df)

        return pd.concat(dfs_list).reset_index(drop = True)

class Game:
    def __init__(self, time, home_team, away_team, score, date, url, season, proxy_obj):
        self.time = time
        self.home_team = home_team
        self.away_team = away_team
        self.score = score
        self.date = date
        self.url = url
        self.season = season
        self.proxy_obj = proxy_obj

    def get_dfs(self):
        header = self.proxy_obj.header
        new_ip = self.proxy_obj.get_new_ip()
        dfs_list = DataRequest(self.url, header, new_ip).get_df
        return dfs_list

    def fill_metadata(self):
        dfs = self.get_dfs()
        home_df, away_df = dfs[0], dfs[1]
        home_df, away_df = home_df.iloc[:-1].copy(), away_df.iloc[:-1].copy()
        home_df['team_name'], home_df['home_away'] = self.home_team, 1
        away_df['team_name'], away_df['home_away'] = self.away_team, 0
        return home_df, away_df

    def concat(self, home_df, away_df):
        df = pd.concat([home_df, away_df]).reset_index(drop = True)
        df['time'] = self.time
        df['score'] = self.score
        df['date'] = self.date
        df['season'] = self.season
        df['url'] = self.url
        return df

class GamesData:
    def __init__(self, games_links_data):
        self.games_links_data = pd.read_csv(games_links_data)
        self.proxy_obj = Proxies()

    def _seasons_list(self):
        return list(self.games_links_data['season'])

    def specific_season_data(self, season):
        return self.games_links_data[self.games_links_data['season'] == season]

    def get_one_season_data(self, season):
        links = self.specific_season_data(season)
        dfs_list = []
        print(f'Iterations: {links.shape[0]}')
        for game_row in tqdm(links.iloc):
            temp_game = Game(*game_row.values, self.proxy_obj)
            home_df, away_df = temp_game.fill_metadata()
            df = temp_game.concat(home_df, away_df)
            dfs_list.append(df)
        return pd.concat(dfs_list)

    def get(self):
        seasons_list = list(self.games_links_data['season'].drop_duplicates())
        results_dfs_list = []
        for season in seasons_list:
            one_season_df = self.get_one_season_data(self, season)
            results_dfs_list.append(one_season_df)
        return pd.concat(results_dfs_list)




# print(GamesData('data/games_links_greece.csv').get_one_season_data(2002))




