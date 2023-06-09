import pandas as pd


def chunk_data(path):
    print('[Processing...]')

    df = pd.read_csv(path)
    df_columns = [i for i in df.columns]

    df['departure'] = pd.to_datetime(df['departure'])
    df['return'] = pd.to_datetime(df['return'])

    df['month'] = df['departure'].dt.strftime('%Y-%m')

    grouped = df.groupby('month')

    for month, data in grouped:
        file_name = f'data/to_load/{month}.csv'
        data.to_csv(file_name, index=False, columns=df_columns)

    print('[---Process finished---]')


chunk_data('data/database.csv')
