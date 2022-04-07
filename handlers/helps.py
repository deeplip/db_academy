import datetime


def whats_datetime():
    return datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')

def save(df, file_name_to_save):
    df.to_csv(file_name_to_save, index = False)