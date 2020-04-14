# Common functions for interacting with the data when producing reports.

import geopandas as gpd
import pandas as pd
from pathlib import Path

def prov_data_paths(data_dir: Path) -> dict:
    """Build a dictionary to map a province to it's GeoPackage file."""

    paths = {}
    for item in data_dir.rglob('*NRN_*.gpkg'):
        parts = item.name.split('_')
        prcode = parts[1]
        major = parts[2]
        minor = parts[3]
        if '.' in minor:
            minor = minor.split('.')[0]
        paths[prcode] = {'path': item, 'major': major, 'minor': minor}
    return paths

def load_roadseg_by_prcode(data_dir: Path, prcode: str) -> gpd.GeoDataFrame:
    """Load the roadseg layer within a GeoPackage into a GeoDataFrame."""

    prov_info = prov_data_paths(data_dir)
    major_version = prov_info[prcode]['major']
    minor_version = prov_info[prcode]['minor']
    gpkg_path = prov_info[prcode]['path']

    layername = f"NRN_{prcode}_{major_version}_{minor_version}_ROADSEG"
    df = (gpd.read_file(gpkg_path, layer=layername)
          .rename(columns=str.lower))

    return df

def load_all_roadseg(data_dir: Path) -> gpd.GeoDataFrame:
    """Load all provinces roadseg layers into a single GeoDataFrame."""

    provs = ['QC','MB','ON','NT','NS','BC','YT','NB','SK','NL','PE','NU','AB']
    roadsegs = []
    for pr in provs:
        df = load_roadseg_by_prcode(data_dir, pr)
        roadsegs.append(df)
    
    return pd.concat(roadsegs)

def date_normalize(value):
    ret_val = value
    if len(value) == 6:
        ret_val = f"{value}01"
    if len(value) == 4:
        ret_val = f"{value}0101"
    return ret_val