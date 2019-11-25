import os
import sys
import geopandas as gpd
import networkx as nx
from datetime import datetime

# start script timer
startTime = datetime.now()

sys.path.insert(1, os.path.join(sys.path[0], ".."))

# read the incoming geopackage from stage 1
# gpkg = gpd.read_file("data/interim/ott_roads_test.gpkg")

# convert the stage 1 geopackage to a shapefile
# gpkg.to_file("data/interim/ott_roads_test.shp", driver='ESRI Shapefile')

# read shapefile
g = nx.read_shp(os.path.abspath("data/interim/ott_roads_test.shp"))

def nodes_dead_ends(g):
    g_dead_end = nx.Graph()
    dead_end = [node for node, degree in g.degree() if degree == 0 or degree == 1]
    g_dead_end.add_nodes_from(dead_end)
    nx.write_shp(g_dead_end, os.path.abspath("data/interim/dead_end.shp"))
    return g_dead_end

nodes_dead_ends(g)

# output execution time
print("Total execution time: ", datetime.now() - startTime)