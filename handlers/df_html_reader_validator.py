import pandas as pd
from handlers import logs
import datetime


def df_html_validator(function):
    def wrapper(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except ValueError as error:
            err_val = error.args[0]
            date = datetime.datetime.now()
            if err_val == 'No tables found':
                logs.logs(date, err_val, 'Lack of tables in html.', False)
                return pd.DataFrame()
            else:
                logs.logs(date, err_val, 'Not know error yet.', False)

    return wrapper
