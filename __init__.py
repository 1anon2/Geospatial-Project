from Core.PathCompare import PathCompare


# WARNING 1: processing MilanoData takes about 3.5 hours using multiprocessing with 8cores.
#            Dataset has 1.8 million rows
# WARNING 2: processing Rome Dataset (Taxi) takes about 3.5 hours using multiprocessing with 8cores.
#            Dataset has +2 million rows
# WARNING 3: processing SF Dataset (Taxi) takes about 3.5 hours using multiprocessing with 8cores.
#            Dataset has +11 million rows

def main():
    path = PathCompare(name='Dataset_Milano/MilanoData.csv', num_of_process=8)
    path.load_graph(graph_area='Milan, Italy', mode='drive')
    path.run(city='Milano')


if __name__ == "__main__":
    main()
