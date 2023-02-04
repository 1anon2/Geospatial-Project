import pandas as pd


class PrepareRome:

    def __init__(self, name):
        self.df = self._read_parse(name)

    @staticmethod
    def _read_parse(name):
        df = pd.DataFrame(pd.read_csv(name, sep=";", names=['userid', 'datetime', 'position']))
        # Randomy select % from original dataset
        main_dataframe = df.sample(frac=0.1)
        # Correction of datetime column, split of position column, change datatype of latitud and longitude
        main_dataframe['datetime'] = main_dataframe['datetime'].str.split(".").str[0]
        # main_dataframe['datetime'] = pd.to_datetime(main_dataframe['datetime'])
        main_dataframe[['lat', 'lng']] = main_dataframe['position'].str.split(' ', expand=True)
        main_dataframe['lat'] = main_dataframe['lat'].str.replace('[POINT(]', '').astype(float)
        main_dataframe['lng'] = main_dataframe['lng'].str.replace('[)]', '').astype(float)
        main_dataframe = main_dataframe.drop(columns=['position'])
        main_dataframe = main_dataframe.reset_index(drop=True)
        return main_dataframe
