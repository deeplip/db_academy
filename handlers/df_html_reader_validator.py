import pandas as pd
import datetime

def df_html_validator(function):
    def wrapper(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except ValueError as error:
            err_val = error.args[0]
            if err_val == 'No tables found':
                print('logs handler here')
                return pd.DataFrame()
            else:
                print('logs handler here')
    return wrapper

