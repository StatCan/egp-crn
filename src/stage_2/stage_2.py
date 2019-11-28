import os, sys
import networkx as nx
import geopandas as gpd
import pandas as pd
import uuid
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

# postgres database url
db_url = URL(drivername='postgresql+psycopg2', host=host, database=db, username=user, port=port, password=pwd)
# engine to connect to postgres
engine = create_engine(db_url)


def main():

#     print(gpkg_in)
#     print(layer_name)
#     print(gpkg_out)
#     gpkg_in = (sys.argv[1])
#     layer_name = (sys.argv[2])
#     gpkg_out = (sys.argv[3])

    # read the incoming geopackage from stage 1
    gpkg_in = gpd.read_file("data/interim/roadseg.gpkg")

    # convert the stage 1 geopackage to a shapefile for networkx usage
    gpkg_in.to_file("data/interim/netx1.shp", driver='ESRI Shapefile')

    # read shapefile
    graph = nx.read_shp("data/interim/netx1.shp")

    # create geodataframe for graph
    graph_gpd = gpd.read_file("data/interim/netx1.shp")

    # import graph into postgis
    graph_gpd.postgis.to_postgis(engine, 'stage_2', 'LineString', if_exists='replace')

    # create empty graph for dead ends
    g_dead_ends = nx.Graph()

    # filter for dead ends
    dead_ends_filter = [node for node, degree in graph.degree() if degree == 1]

    # add filter to empty graph
    g_dead_ends.add_nodes_from(dead_ends_filter)

    # SQL query to create junctions (JUNCTYPE=Intersection)
    sql = """
    WITH inter AS (SELECT ST_Intersection(a.geom, b.geom) geom, 
                      Count(DISTINCT a.index) 
               FROM   stage_2 AS a, 
                      stage_2 AS b 
               WHERE  ST_Touches(a.geom, b.geom)
                      AND a.index != b.index 
               GROUP  BY ST_Intersection(a.geom, b.geom))
          SELECT * FROM inter WHERE count > 2;
    """

    # create junctions geodataframe
    inter = gpd.GeoDataFrame.from_postgis(sql, engine)

    nx.write_shp(g_dead_ends, "data/interim/dead_end.shp")

    inter.to_file("data/interim/intersections.gpkg", driver='GPKG')

    dead_ends_gpd = gpd.read_file("data/interim/dead_end.shp")
    dead_ends_gpd["junctype"] = 'Dead End'

    intersections_gpd = gpd.read_file("data/interim/intersections.gpkg")
    intersections_gpd["junctype"] = 'Intersection'

    junctions = gpd.GeoDataFrame(pd.concat([dead_ends_gpd, intersections_gpd], sort=False))
    junctions = junctions[['junctype', 'geometry']]

    junctions["nid"] = [uuid.uuid4() for i in range(len(junctions))]
    junctions["nid"] = junctions["nid"].astype(str)
    junctions["nid"] = junctions["nid"].replace('-', '', regex=True)

    junctions["datasetnam"] = "New Brunswick"
    junctions["specvers"] = "2.0"
    junctions["accuracy"] = 10
    junctions["acqtech"] = "Computed"
    junctions["provider"] = "Provincial/Territorial"
    junctions["credate"] = "20191127"
    junctions["revdate"] = "20191127"
    junctions["metacover"] = "Complete"
    junctions["exitnbr"] = ""
    junctions["junctype"] = junctions["junctype"]

    junctions.to_file("data/interim/nb.gpkg", driver='GPKG')

    # read the incoming geopackage from stage 1
    ferry = gpd.read_file("data/interim/ferryseg.gpkg")

    # convert the stage 1 geopackage to a shapefile for networkx usage
    ferry.to_file("data/interim/netx2.shp", driver='ESRI Shapefile')

    # read shapefile
    ferry_g = nx.read_shp("data/interim/netx2.shp")

    # create empty graph for dead ends
    ferry_graph = nx.Graph()

    # filter for dead ends
    ferry_filter = [node for node, degree in ferry_g.degree() if degree > 0]

    # add filter to empty graph
    ferry_graph.add_nodes_from(ferry_filter)

    nx.write_shp(ferry_graph, "data/interim/ferry.shp")

    ferry_gpd = gpd.read_file("data/interim/ferry.shp")

    subset = junctions.geometry.map(lambda x: x.equals(ferry_gpd.geometry.any()))

    # subset.to_file("data/interim/subset.gpkg", driver="GPKG")

    print(subset)


if __name__ == "__main__":

    # if len(sys.argv) != 1:
    #     print("ERROR: You must supply 3 arguments. "
    #           "Example: python stage_2.py [INPUT GPKG] [LAYER NAME] [OUTPUT GPKG]")
    #     sys.exit(1)

    main()

# output execution time
print("Total execution time: ", datetime.now() - startTime)