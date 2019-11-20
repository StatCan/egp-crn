import geopandas as gpd
import time
from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# start time for script timer
start = time.time()

# postgres connection parameters
HOST = 'localhost'
DB = 'nrn'
USER = 'postgres'
PORT = 5432
PWD = 'password'

# postgres database url
db_url = URL(drivername='postgresql+psycopg2', host=HOST, database=DB, username=USER, port=PORT, password=PWD)

# engine to connect to postgres
engine = create_engine(db_url)

# create database session
Session = sessionmaker(bind=engine)
session = Session()

# SQL query to create junctions (JUNCTYPE=Intersection)
sql = "WITH ott_junc AS (SELECT ST_Intersection(a.geom, b.geom) geom, " \
                  "Count(DISTINCT a.objectid) " \
           "FROM   ott AS a, " \
                  "ott AS b " \
           "WHERE  ST_Touches(a.geom, b.geom) " \
                  "AND a.objectid != b.objectid " \
           "GROUP  BY ST_Intersection(a.geom, b.geom))" \
      "SELECT * FROM ott_junc WHERE count > 2;"

# create junctions geodataframe
junc = gpd.GeoDataFrame.from_postgis(sql, engine)

# close database connection
session.close()

# output execution time
end = time.time()
hours, rem = divmod(end-start, 3600)
minutes, seconds = divmod(rem, 60)
print("{:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds))