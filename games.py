from handlers.links import Links
from handlers.data_request import DataRequest
import pandas as pd
from tqdm import tqdm


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
            df['link'] = get_links()
        except:
            df['link'] = ''

        return df

    def all_seasons_data(self):

        def get_pages(data_request_object):
            soup = data_request_object.get_soup
            return len(soup.find_all("a", {"class": "pagination-link"}))

        links = self._get_all_seasons_links()
        dfs_list = []
        header = self.proxy_obj.header
        for link in tqdm(links):
            data_request_object = DataRequest(link, header, self.proxy_obj.get_new_ip())
            page_one_dfs = self._one_page_one_season_data(data_request_object)
            dfs_list.append(page_one_dfs)
            pages = get_pages(data_request_object)

            for page in range(pages):
                data_request_object = DataRequest(link + f'/{page}', header, self.proxy_obj.get_new_ip())
                temp_df = self._one_page_one_season_data(data_request_object)
                dfs_list.append(temp_df)

        return pd.concat(dfs_list).reset_index(drop = True)

    def get_and_save(self, file_name_to_save):
        df = self.all_seasons_data()
        df.to_csv(file_name_to_save)
