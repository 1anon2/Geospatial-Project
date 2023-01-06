import multiprocessing
from datetime import datetime

import pandas as pd
import osmnx as ox
import skmob
import networkx as nx
from skmob.preprocessing import detection


class PathCompare:

    def __init__(self, name, num_of_process=4):
        self._num_of_process = num_of_process
        ox.config(use_cache=True, log_console=True)
        self._df = pd.read_csv(name, engine='python', sep=',')
        self._unique_users = list(self._df['userid'].unique())
        self._df.rename(columns={'lon': 'lng'}, inplace=True)
        self.G = None

    def load_graph(self, graph_area, mode):
        self.G = ox.graph_from_place(graph_area, network_type=mode, clean_periphery=True, retain_all=True)
        G_proj = ox.projection.project_graph(self.G)
        self.G = ox.utils_graph.get_largest_component(self.G, strongly=True)
        nodes_proj = ox.graph_to_gdfs(G_proj, edges=False)
        # graph_area_m = nodes_proj.unary_union.convex_hull.area
        # impute missing edge speeds and calculate edge travel times with the speed module
        self.G = ox.speed.add_edge_speeds(self.G)
        self.G = ox.speed.add_edge_travel_times(self.G)

    def _preprocessing(self, user):
        df_aux = self._df.loc[self._df['userid'] == user]
        # Create TrajDataFrame
        tdf_01 = skmob.TrajDataFrame(df_aux, latitude='lat', longitude='lng', user_id='userid', datetime='datetime')
        # PREPROCESSING: Filtering
        tdf_02 = skmob.preprocessing.filtering.filter(tdf_01, max_speed_kmh=500.)
        # PREPROCESSING: compression. Reduces the quantity of GPS points of every user
        tdf_03 = skmob.preprocessing.compression.compress(tdf_02, spatial_radius_km=0.1)
        # PREPROCESSING: Split trajectories into sub trajectories. Returns a TDF with only the stops
        tdf_04 = detection.stay_locations(tdf_03, stop_radius_factor=0.3, minutes_for_a_stop=10.0,
                                          spatial_radius_km=0.2)
        return tdf_03, tdf_04

    def _routing(self, tdf_03, tdf_04):
        tdf_03_nodes = []
        tdf_03_route = []
        orig_dest_route = []
        length_m_calc = 0
        length_m_real = 0
        tdf_04_len = len(tdf_04)

        for i, elem in tdf_04.iterrows():
            if i + 1 == tdf_04_len:
                break
                # Step 1
            orig_node = ox.distance.nearest_nodes(self.G, Y=elem.lat, X=elem.lng)
            dest_node = ox.distance.nearest_nodes(self.G, Y=tdf_04.loc[i + 1].lat, X=tdf_04.loc[i + 1].lng)
            orig_dest_route = orig_dest_route + ox.shortest_path(self.G, orig_node, dest_node, weight="length")

            # Step 2
            length_m_calc = length_m_calc + round(
                nx.shortest_path_length(self.G, source=orig_node, target=dest_node, weight='length'))

            # Step 3
            orig_datetime = pd.to_datetime(elem.datetime)
            dest_datetime = pd.to_datetime(tdf_04.loc[i + 1].datetime)

            tdf_03_GPS = tdf_03.loc[(tdf_03['datetime'] >= orig_datetime) & (tdf_03['datetime'] <= dest_datetime)]
            tdf_03_nodes.append(ox.distance.nearest_nodes(self.G, Y=tdf_03_GPS.lat, X=tdf_03_GPS.lng))

        # Step 4
        for i in tdf_03_nodes:
            for j in range(0, len(i) - 1):
                tdf_03_route = tdf_03_route + ox.shortest_path(self.G, i[j], i[j + 1], weight='length')[:-1]
                length_m_real = length_m_real + round(
                    nx.shortest_path_length(self.G, source=i[j], target=i[j + 1], weight='length'))
            tdf_03_route.append(i[j])
        tdf_03_route.append(tdf_03_nodes[-1][-1])

        return tdf_03_nodes, tdf_03_route, orig_dest_route, int(length_m_calc), int(length_m_real)

    @staticmethod
    def _intersection(s1, s2):
        # Find the intersection of the two sets
        intersect = s1 & s2
        return intersect

    # Function to return the Jaccard index of two sets
    def jaccard_index(self, s1, s2):
        size_s1 = len(s1)
        size_s2 = len(s2)
        # Get the intersection set
        intersect = self._intersection(s1, s2)
        # Size of the intersection set
        size_in = len(intersect)
        # using the formula
        jaccard_in = round(size_in / (size_s1 + size_s2 - size_in), 3)
        return jaccard_in

    def _worker(self, user):
        data_03, data_04 = self._preprocessing(self._df, user)
        data_03_nodes, route_real, route_calc, length_calc, length_real = self._routing(self.G, data_03, data_04)
        s1 = set(route_real)
        s2 = set(route_calc)
        metrics_pd = pd.DataFrame(
            {
                'Jaccard': [self.jaccard_index(s1, s2)],
                'length_real': [length_real],
                'length_calc': [length_calc],
                'user': [user]
            }
        )
        metrics_pd['length_dif'] = abs(metrics_pd['length_real'] - metrics_pd['length_calc'])
        return metrics_pd

    def run(self, city=''):
        df = pd.DataFrame(columns=['Jaccard', 'length_real', 'length_calc', 'user', 'length_dif'])
        with multiprocessing.Pool(self._num_of_process) as pool:
            df.append(pool.map(self._worker, self._unique_users))
        current_date_time = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        df.to_csv(f"Metrics_{city}_{current_date_time}.csv", index=False, sep=';')
