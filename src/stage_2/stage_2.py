import os, sys
import networkx as nx
import geopandas as gpd
from datetime import datetime
from sqlalchemy import *
from sqlalchemy.engine.url import URL
from geopandas_postgis import PostGIS
sys.path.insert(1, os.path.join(sys.path[0], ".."))

# start script timer
startTime = datetime.now()

# postgres connection parameters
host = 'localhost'
db = 'nrn'
user = 'postgres'
port = 5432
pwd = 'password'

# read the incoming geopackage from stage 1
# gpkg = gpd.read_file("data/interim/.gpkg")

# convert the stage 1 geopackage to a shapefile for networkx usage
# gpkg.to_file("data/interim/.shp", driver='ESRI Shapefile')

input = "C:/Users/jacoken/data/NRN/NRN_RRN_ON_12_0_SHAPE/NRN_ON_12_0_SHAPE_en/NRN_ON_12_0_ROADSEG.shp"

# read shapefile
graph = nx.read_shp(os.path.abspath(input))

# create geodataframe for graph
graph_gpd = gpd.read_file(input)

# postgres database url
db_url = URL(drivername='postgresql+psycopg2', host=host, database=db, username=user, port=port, password=pwd)

# engine to connect to postgres
engine = create_engine(db_url)

# import graph into postgis
graph_gpd.postgis.to_postgis(engine, 'stage_2', 'LineString', if_exists='replace')

# create empty graph for dead ends
g_dead_ends = nx.Graph()

# filter for dead ends
dead_ends_filter = [node for node, degree in graph.degree() if degree == 0 or degree == 1]

# add filter to empty graph
g_dead_ends.add_nodes_from(dead_ends_filter)

# SQL query to create junctions (JUNCTYPE=Intersection)
sql = "WITH inter AS (SELECT ST_Intersection(a.geom, b.geom) geom, " \
                  "Count(DISTINCT a.index) " \
           "FROM   stage_2 AS a, " \
                  "stage_2 AS b " \
           "WHERE  ST_Touches(a.geom, b.geom) " \
                  "AND a.index != b.index " \
           "GROUP  BY ST_Intersection(a.geom, b.geom))" \
      "SELECT * FROM inter WHERE count > 2;"

# create junctions geodataframe
inter = gpd.GeoDataFrame.from_postgis(sql, engine)

nx.write_shp(g_dead_ends, os.path.abspath("data/interim/dead_end.shp"))
dead_ends_gpd = gpd.read_file("data/interim/dead_end.shp")
dead_ends_gpd.to_file("data/interim/junctions.gpkg", layer='deadends', driver='GPKG')
inter.to_file("data/interim/junctions.gpkg", layer='intersections', driver='GPKG')

# output execution time
print("Total execution time: ", datetime.now() - startTime)