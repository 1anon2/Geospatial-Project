from Core.PathCompare import PathCompare


def main():
    path = PathCompare(name='MilanoData.csv')
    path.load_graph(graph_area='Milan, Italy', mode='drive')
    path.run()


if __name__ == "__main__":
    main()
