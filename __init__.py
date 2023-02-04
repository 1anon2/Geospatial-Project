from Core.PathCompare import PathCompare
from Utils.PrepareRome import PrepareRome
from Utils.PrepareSanFrancisco import PrepareSanFrancisco


# WARNING 1: processing MilanoData takes about 3.5 hours using multiprocessing with 8cores.
#            Dataset has 1.8 million rows
# WARNING 2: processing Rome Dataset (Taxi) takes about 3.5 hours using multiprocessing with 8cores.
#            Dataset has +2 million rows
# WARNING 3: processing SF Dataset (Taxi) takes about 3.5 hours using multiprocessing with 8cores.
#            Dataset has +11 million rows


def main_milano():
    path = PathCompare(name='Dataset_Milano/MilanoData.csv', num_of_process=8)
    path.load_graph(graph_area='Milan, Italy', mode='drive')
    path.run(city='Milano')


def main_san_gianfranco():
    path = PathCompare(num_of_process=8)
    obj_san_f = PrepareSanFrancisco(name='Dataset_San_Francisco')
    path.set_df(obj_san_f.df)
    path.load_graph(graph_area='San Francisco, USA', mode='drive')
    path.run(city='san_francisco')


def main_roma():
    path = PathCompare(num_of_process=8)
    obj_rome = PrepareRome(name='Dataset_Rome_Taxi/taxi_february.txt')
    path.set_df(obj_rome.df)
    path.load_graph(graph_area='Rome, Italy', mode='drive')
    path.run(city='Rome')


if __name__ == "__main__":
    main_roma()
    main_san_gianfranco()
