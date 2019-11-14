import geopandas as gpd
import psycopg2
import time

start = time.time()

con = psycopg2.connect(database="nrn", user="postgres", password="password", host="localhost")

sql = "WITH nbjunc AS (SELECT ST_Intersection(a.geom, b.geom) geom, " \
                  "Count(DISTINCT a.primaryindex) " \
           "FROM   nb AS a, " \
                  "nb AS b " \
           "WHERE  ST_Touches(a.geom, b.geom) " \
                  "AND a.primaryindex != b.primaryindex " \
           "GROUP  BY ST_Intersection(a.geom, b.geom))" \
      "SELECT * FROM nbjunc WHERE count > 2;"

junc = gpd.GeoDataFrame.from_postgis(sql, con)

end = time.time()
hours, rem = divmod(end-start, 3600)
minutes, seconds = divmod(rem, 60)
print("{:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds))