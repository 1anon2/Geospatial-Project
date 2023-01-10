from Core.PathCompare import PathCompare

# WARNING: processing MilanoData takes about 3.5 hours using multiprocessing with 8cores

def main():
    path = PathCompare(name='MilanoData.csv', num_of_process=8)
    path.load_graph(graph_area='Milan, Italy', mode='drive')
    path.run(city='Milano')


if __name__ == "__main__":
    main()
