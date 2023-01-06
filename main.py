import pandas as pd
import skmob
from skmob.preprocessing import detection
import osmnx as ox
import networkx as nx
import warnings
from datetime import datetime


ox.config(use_cache=True, log_console=True)

# LOAD DATASET
pd_milano = pd.read_csv('MilanoData.csv', engine='python', sep=',')
pd_milano.rename(columns={'lon': 'lng'}, inplace=True)

# LOAD GRAPHS
# Only strongly connected nodes are taken.
# Weakly connected nodes are discarded because it causes errors due to some points not being reachable
graph_area = 'Milan, Italy'
mode = 'drive'
G = ox.graph_from_place(graph_area, network_type=mode, clean_periphery=True, retain_all=True)
G_proj = ox.projection.project_graph(G)
G = ox.utils_graph.get_largest_component(G, strongly=True)
nodes_proj = ox.graph_to_gdfs(G_proj, edges=False)
graph_area_m = nodes_proj.unary_union.convex_hull.area
# impute missing edge speeds and calculate edge travel times with the speed module
G = ox.speed.add_edge_speeds(G)
G = ox.speed.add_edge_travel_times(G)


# PREPROCESSING DATA
# The following function is to create 2 DF:
# 1.- tdf_03 contains all the GPS points after cleanup
# 2.- tdf_04 contains only the stops detected.
# In order to complete the tasks, between each stop is necessary to delect all the GPS points
# that fall inside the origin and destination point. This is done using the datetime
def preprocessing(pd, j):
    pd_milano_01 = pd_milano.loc[pd_milano['userid'] == j]
    # Create TrajDataFrame
    tdf_01 = skmob.TrajDataFrame(pd_milano_01, latitude='lat', longitude='lng', user_id='userid', datetime='datetime')

    # PREPROCESSING: Filtering
    tdf_02 = skmob.preprocessing.filtering.filter(tdf_01, max_speed_kmh=500.)

    # PREPROCESSING: compression. Reduces the quantity of GPS points of every user
    tdf_03 = skmob.preprocessing.compression.compress(tdf_02, spatial_radius_km=0.1)

    # PREPROCESSING: Split trajectories into sub trajectories. Returns a TDF with only the stops
    tdf_04 = detection.stay_locations(tdf_03, stop_radius_factor=0.3, minutes_for_a_stop=10.0, spatial_radius_km=0.2)
    return tdf_03, tdf_04


# ROUTE AND METRICS CREATION
# The following function does the following:
# 1.- For each subtrajectory, it takes the orig and dest points, calculates the nearest node,
#     calculates the shortest path and appends that information in a list called orig_dest_route
# 2.- Appends the distance calculated in the previous step (in meters) into a list called length_m
# 3.- For each subtrajectory, selects from the GPS points between stops (from tdf_03), calculates the nearest node
#     and appends the result in tdf_03_nodes.
# 4.- Iterates every nodes from tdf_03_nodes and computes the shortest path and the distance

def routing(G, tdf_03, tdf_04):
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
        orig_node = ox.distance.nearest_nodes(G, Y=elem.lat, X=elem.lng)
        dest_node = ox.distance.nearest_nodes(G, Y=tdf_04.loc[i + 1].lat, X=tdf_04.loc[i + 1].lng)
        orig_dest_route = orig_dest_route + ox.shortest_path(G, orig_node, dest_node, weight="length")

        # Step 2
        length_m_calc = length_m_calc + round(
            nx.shortest_path_length(G, source=orig_node, target=dest_node, weight='length'))

        # Step 3
        orig_datetime = pd.to_datetime(elem.datetime)
        dest_datetime = pd.to_datetime(tdf_04.loc[i + 1].datetime)

        tdf_03_GPS = tdf_03.loc[(tdf_03['datetime'] >= orig_datetime) & (tdf_03['datetime'] <= dest_datetime)]
        tdf_03_nodes.append(ox.distance.nearest_nodes(G, Y=tdf_03_GPS.lat, X=tdf_03_GPS.lng))

    # Step 4
    for i in tdf_03_nodes:
        for j in range(0, len(i) - 1):
            tdf_03_route = tdf_03_route + ox.shortest_path(G, i[j], i[j + 1], weight='length')[:-1]
            length_m_real = length_m_real + round(
                nx.shortest_path_length(G, source=i[j], target=i[j + 1], weight='length'))
        tdf_03_route.append(i[j])
    tdf_03_route.append(tdf_03_nodes[-1][-1])

    return tdf_03_nodes, tdf_03_route, orig_dest_route, int(length_m_calc), int(length_m_real)


# JACCARD INDEX
# Function to return the intersection set of s1 and s2
def intersection(s1, s2):
    # Find the intersection of the two sets
    intersect = s1 & s2
    return intersect


# Function to return the Jaccard index of two sets
def jaccard_index(s1, s2):
    size_s1 = len(s1)
    size_s2 = len(s2)
    # Get the intersection set
    intersect = intersection(s1, s2)
    # Size of the intersection set
    size_in = len(intersect)
    # using the formula
    jaccard_in = round(size_in / (size_s1 + size_s2 - size_in), 3)
    return jaccard_in


# RUN THE PROGRAM!!
users = pd_milano['userid'].unique()
users = [193, 1059]
jaccard_list = list()
length_real_list = list()
length_calc_list = list()
user_list = list()

for i, j in enumerate(users):
    data_03, data_04 = preprocessing(pd_milano, j)
    data_03_nodes, route_real, route_calc, length_calc, length_real = routing(G, data_03, data_04)

    s1 = set(route_real)
    s2 = set(route_calc)
    jaccardIndex = jaccard_index(s1, s2)
    jaccard_list.append(jaccardIndex)
    length_real_list.append(length_real)
    length_calc_list.append(length_calc)
    user_list.append(j)

# TO PANDAS AND EXPORT!
metrics_pd = pd.DataFrame({'Jaccard': jaccard_list,
                           'length_real': length_real_list,
                           'length_calc': length_calc_list,
                           'user': user_list})
metrics_pd['length_dif'] = abs(metrics_pd['length_real'] - metrics_pd['length_calc'])

currentDateTime = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
metrics_pd.to_csv(f"Metrics_{currentDateTime}.csv", index=False, sep=';')

# ANALYSIS OF RESULTS
