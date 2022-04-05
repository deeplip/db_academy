import players
league_name = 'greece'
links_file_name = f'data/players_links_{league_name}.csv'

def fetch_players_links(main_url, file_name_to_save):
    links_df = players.PlayersLinks(main_url).all_seasons_data()
    links_df.to_csv(file_name_to_save, index = False)
    return links_df

greece = 'https://www.proballers.com/basketball/league/50/greece-heba-a1/players/'
fetch_players_links(greece, links_file_name)

def fetch_players_data(league_name, links_file_name):
    players_data_obj = players.PlayersData(league_name, links_file_name)
    players_data_obj.get_and_save()
    return players_data_obj

fetch_players_data(league_name, links_file_name)
