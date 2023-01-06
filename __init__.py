from Core.PathCompare import PathCompare


def main():
    path = PathCompare(name='MilanoData.csv', num_of_process=7)
    path.load_graph(graph_area='Milan, Italy', mode='drive')
    path.run(city='Milano')


if __name__ == "__main__":
    main()
