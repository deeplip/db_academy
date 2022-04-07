import players
import games
from handlers import helps


def fetch_players_links(main_url, file_name_to_save):
    df = players.PlayersLinks(main_url).all_seasons_data()
    helps.save(df, file_name_to_save)


def fetch_players_data(file_data_with_players_links, file_name_to_save):
    players_data_obj = players.PlayersData(file_data_with_players_links)
    df = players_data_obj.get()
    helps.save(df, file_name_to_save)


def fetch_games_links(main_url, file_name_to_save):
    df = games.GamesLinks(main_url).get()
    helps.save(df, file_name_to_save)


# fetch_players_links('https://www.proballers.com/basketball/league/50/greece-heba-a1/players/',
#                     f'data/players_links_greece.csv')
# fetch_players_data(f'data/players_links_greece.csv', f'data/players_data_greece_test.csv')
fetch_games_links('https://www.proballers.com/basketball/league/50/greece-heba-a1/schedule/',
                  f'data/games_links_greece_test.csv')
