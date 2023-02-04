import glob

import pandas as pd


class PrepareSanFrancisco:

    def __init__(self, name):
        self.df = self._prepare(name)

    @staticmethod
    def _prepare(name):
        file_list = glob.glob('%s/new_*.txt' % name)
        main_dataframe = pd.DataFrame(
            pd.read_csv(file_list[0], sep=" ", names=['lat', 'lng', 'occupancy', 'datetime', 'userid']))

        main_dataframe = main_dataframe.sample(frac=0.1)
        # main_dataframe['userid'] = int(0)

        for i in range(1, len(file_list)):
            data = pd.read_csv(file_list[i], sep=" ", names=['lat', 'lng', 'occupancy', 'datetime'])
            df = pd.DataFrame(data)
            df['userid'] = int(i)
            main_dataframe = pd.concat([main_dataframe, df])

        main_dataframe['datetime'] = pd.to_datetime(main_dataframe['datetime'], unit='s')
        return main_dataframe
