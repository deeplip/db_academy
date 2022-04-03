import players

def fetch_players_links(main_url, league):
    links_df = players.PlayersLinks(main_url).all_seasons_data()
    links_df.to_csv(f'data/players_links_{league}.csv', index = False)
    return links_df

greece = 'https://www.proballers.com/basketball/league/50/greece-heba-a1/players/'
fetch_players_links(greece, 'greece')

