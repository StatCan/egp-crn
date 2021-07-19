import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine


# placeholder, nothing here is sensitive. Just a generic user and password.
db_url = "postgresql://postgres:postgres@localhost:5433/nrn"
con = create_engine(db_url)
gdf = gpd.read_postgis("select * from public.segment;", con, geom_col="geometry")
df = pd.read_sql_query("select * from public.acquisition_technique_lookup;", con)