import os
import sys
import networkx as nx
import geopandas as gpd
import pandas as pd
import uuid
from datetime import datetime
from sqlalchemy import *
from sqlalchemy.engine.url import URL
from shapely.geometry.multipoint import MultiPoint
from shapely.geometry.point import Point
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
    gpkg_in = gpd.read_file("data/raw/Testing_Data/NB.gpkg", layer="ROADSEG")

    # convert the stage 1 geopackage to a shapefile for networkx usage
    gpkg_in.to_file("data/raw/netx1.shp", driver='ESRI Shapefile')

    # read shapefile
    graph = nx.read_shp("data/raw/netx1.shp")

    # create geodataframe for graph
    graph_gpd = gpd.read_file("data/raw/netx1.shp")

    # import graph into postgis
    graph_gpd.postgis.to_postgis(con=engine, table_name='stage_2', geometry='LineString', if_exists='replace')

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

    nx.write_shp(g_dead_ends, "data/raw/dead_end.shp")

    inter.to_file("data/raw/intersections.gpkg", driver='GPKG')

    dead_ends_gpd = gpd.read_file("data/raw/dead_end.shp")
    dead_ends_gpd["junctype"] = 'Dead End'

    intersections_gpd = gpd.read_file("data/raw/intersections.gpkg")
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
    junctions["provider"] = "Provincial / Territorial"
    junctions["credate"] = "20191127"
    junctions["revdate"] = "20191127"
    junctions["metacover"] = "Complete"
    junctions["exitnbr"] = ""
    junctions["junctype"] = junctions["junctype"]

    junctions.to_file("data/raw/nb.gpkg", driver='GPKG')

    # read the incoming geopackage from stage 1
    ferry = gpd.read_file("data/raw/Testing_Data/NB.gpkg", layer="FERRYSEG")

    # convert the stage 1 geopackage to a shapefile for networkx usage
    ferry.to_file("data/raw/netx2.shp", driver='ESRI Shapefile')

    # read shapefile
    ferry_g = nx.read_shp("data/raw/netx2.shp")

    # create empty graph for dead ends
    ferry_graph = nx.Graph()

    # filter for dead ends
    ferry_filter = [node for node, degree in ferry_g.degree() if degree > 0]

    # add filter to empty graph
    ferry_graph.add_nodes_from(ferry_filter)

    nx.write_shp(ferry_graph, "data/raw/ferry.shp")

    ferry_junc = gpd.read_file("data/raw/ferry.shp")
    merged_junc = gpd.read_file("data/raw/nb.gpkg")

    ferry_junc.crs = {'init': 'epsg:4617'}
    merged_junc.crs = {'init': 'epsg:4617'}

    # https://gis.stackexchange.com/questions/311320/casting-geometry-to-multi-using-geopandas
    merged_junc["geometry"] = [MultiPoint([feature]) if type(feature) == Point else feature for feature in merged_junc["geometry"]]
    ferry_junc["geometry"] = [MultiPoint([feature]) if type(feature) == Point else feature for feature in ferry_junc["geometry"]]

    # import graph into postgis
    merged_junc.postgis.to_postgis(con=engine, table_name='stage_2_junc', geometry='MULTIPOINT', if_exists='replace')
    ferry_junc.postgis.to_postgis(con=engine, table_name='stage_2_ferry_junc', geometry='MULTIPOINT', if_exists='replace')

    sql_junc = """
    DROP TABLE nb_junc_merge;
    CREATE TABLE nb_junc_merge AS (
      SELECT
        a.geom,
        a.nid,
        a.datasetnam,
        a.specvers,
        a.accuracy,
        a.acqtech,
        a.provider,
        a.credate,
        a.revdate,
        a.metacover,
        a.exitnbr,
        a.junctype,
        b.index AS b_index,
        c.id AS c_index,
        d.id AS d_index
      FROM
        stage_2_junc a
        LEFT JOIN stage_2_ferry_junc b ON ST_Equals(a.geom, b.geom) 
        LEFT JOIN neigh c ON ST_Intersects(a.geom, c.geom)
        LEFT JOIN nb_adm d ON ST_Within(a.geom, d.geom));
    UPDATE nb_junc_merge SET junctype = 'Ferry' WHERE b_index IS NOT NULL;
    UPDATE nb_junc_merge SET junctype = 'NatProvTer' WHERE c_index IS NOT NULL;
    UPDATE nb_junc_merge SET junctype = 'NatProvTer' WHERE d_index IS NULL;
    ALTER TABLE nb_junc_merge DROP COLUMN b_index, DROP COLUMN c_index, DROP COLUMN d_index;
    SELECT * FROM nb_junc_merge;
    """

    merged_junctions = gpd.GeoDataFrame.from_postgis(sql_junc, engine)

    merged_junctions.to_file("data/raw/merged_junc.gpkg", driver='GPKG')

if __name__ == "__main__":

    # if len(sys.argv) != 1:
    #     print("ERROR: You must supply 3 arguments. "
    #           "Example: python stage_2.py [INPUT GPKG] [LAYER NAME] [OUTPUT GPKG]")
    #     sys.exit(1)

    main()

# output execution time
print("Total execution time: ", datetime.now() - startTime)