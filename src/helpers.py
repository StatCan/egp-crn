import datetime
import fiona
import geopandas as gpd
import logging
import networkx as nx
import numpy as np
import pandas as pd
import random
import re
import requests
import sqlite3
import sys
import time
import yaml
from collections import defaultdict
from operator import attrgetter, itemgetter
from osgeo import ogr, osr
from pathlib import Path
from shapely.geometry import LineString, Point
from shapely.wkt import loads
from tqdm import tqdm
from tqdm.auto import trange
from typing import Any, Dict, List, Tuple, Union


logger = logging.getLogger()
ogr.UseExceptions()


# Define universally accessible variables.
filepath = Path(__file__).resolve()
distribution_format_path = filepath.parent / "distribution_format.yaml"
field_domains_path = {lang: filepath.parent / f"field_domains_{lang}.yaml" for lang in ("en", "fr")}


class TempHandlerSwap:
    """Temporarily swaps all Logger StreamHandlers with a FileHandler."""

    def __init__(self, class_logger: logging.Logger, log_path: Union[Path, str]) -> None:
        """
        Initializes the TempHandlerSwap contextmanager class.

        :param logging.Logger class_logger: Logger.
        :param Union[Path, str] log_path: path where the FileHandler will write logs to.
        """

        self.logger = class_logger
        self.log_path = Path(log_path).resolve()

        # Store stream handlers.
        self.stream_handlers = [h for h in self.logger.handlers if isinstance(h, logging.StreamHandler)]

        # Define file handler.
        self.file_handler = logging.FileHandler(self.log_path)
        self.file_handler.setLevel(logging.INFO)
        self.file_handler.setFormatter(self.logger.handlers[0].formatter)

    def __enter__(self) -> None:
        """Removes all StreamHandlers and adds a FileHandler."""

        logger.info(f"Temporarily redirecting stream logging to file: {self.log_path}.")

        for handler in self.stream_handlers:
            self.logger.removeHandler(handler)
        self.logger.addHandler(self.file_handler)

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """
        Removes the FileHandler and restores the original StreamHandlers.

        :param Any exc_type: required parameter for __exit__.
        :param Any exc_val: required parameter for __exit__.
        :param Any exc_tb: required parameter for __exit__.
        """

        for handler in self.stream_handlers:
            self.logger.addHandler(handler)
        self.logger.removeHandler(self.file_handler)

        logger.info("File logging complete; reverted logging to stream.")


class Timer:
    """Tracks stage runtime."""

    def __init__(self) -> None:
        """Initializes the Timer class."""

        self.start_time = None

    def __enter__(self) -> None:
        """Starts the timer."""

        logger.info("Started.")
        self.start_time = time.time()

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """
        Computes and returns the elapsed time.

        :param Any exc_type: required parameter for __exit__.
        :param Any exc_val: required parameter for __exit__.
        :param Any exc_tb: required parameter for __exit__.
        """

        total_seconds = time.time() - self.start_time
        delta = datetime.timedelta(seconds=total_seconds)
        logger.info(f"Finished. Time elapsed: {delta}.")


def apply_domain(series: pd.Series, domain: dict, default: Any) -> pd.Series:
    """
    Applies a domain restriction to the given Series based on a domain dictionary.
    Replaces missing or invalid values with the default value.

    Non-dictionary domains are treated as Null. Values are left as-is excluding Null types and empty strings, which are
    replaced with the default value.

    :param pd.Series series: Series.
    :param dict domain: dictionary of acceptable domain values.
    :param Any default: default value.
    :return pd.Series: Series with enforced domain restriction.
    """

    # Validate against domain dictionary.
    if isinstance(domain, dict):

        # Convert keys to lowercase strings.
        domain = {str(k).lower(): v for k, v in domain.items()}

        # Configure lookup function, convert invalid values to default.
        def get_value(val: Any) -> Any:
            """
            Retrieves a domain dictionary value for a given key, non-matches return the default value.

            :param Any val: lookup key.
            :return Any: corresponding domain value or the default value.
            """

            try:
                return domain[str(val).lower()]
            except KeyError:
                return default

        # Get values.
        return series.map(get_value)

    else:

        # Convert empty strings and null types to default.
        series.loc[(series.map(str).isin(["", "nan"])) | (series.isna())] = default
        return series


def compile_default_values(lang: str = "en") -> dict:
    """
    Compiles the default value for each field in each NRN dataset.

    :param str lang: output language: 'en', 'fr'.
    :return dict: dictionary of default values for each attribute of each NRN dataset.
    """

    dft_vals = load_yaml(field_domains_path[lang])["default"]
    dist_format = load_yaml(distribution_format_path)
    defaults = dict()

    try:

        # Iterate tables.
        for name in dist_format:
            defaults[name] = dict()

            # Iterate fields.
            for field, dtype in dist_format[name]["fields"].items():

                # Configure default value.
                key = "label" if dtype[0] == "str" else "code"
                defaults[name][field] = dft_vals[key]

    except (AttributeError, KeyError, ValueError):
        logger.exception(f"Invalid schema definition for one or more yamls:"
                         f"\nDefault values: {dft_vals}"
                         f"\nDistribution format: {dist_format}")
        sys.exit(1)

    return defaults


def compile_domains(mapped_lang: str = "en") -> dict:
    """
    Compiles the acceptable domain values for each field in each NRN dataset. Each domain will consist of the following
    keys:
    1) 'values': all English and French values and keys flattened into a single list.
    2) 'lookup': a lookup dictionary mapping each English and French value and key to the value of the given map
    language. Integer keys and their float-equivalents are both added to accommodate incorrectly casted data.

    :param str mapped_lang: output language: 'en', 'fr'.
    :return dict: dictionary of domain values and lookup dictionary for each attribute of each NRN dataset.
    """

    # Compile field domains.
    domains = defaultdict(dict)

    # Load domain yamls.
    domain_yamls = {lang: load_yaml(field_domains_path[lang]) for lang in ("en", "fr")}

    # Iterate tables and fields with domains.
    for table in domain_yamls["en"]["tables"]:
        for field in domain_yamls["en"]["tables"][table]:

            try:

                # Compile domains.
                domain_en = domain_yamls["en"]["tables"][table][field]
                domain_fr = domain_yamls["fr"]["tables"][table][field]

                # Configure mapped and non-mapped output domain.
                domain_mapped = domain_en if mapped_lang == "en" else domain_fr
                domain_non_mapped = domain_en if mapped_lang != "en" else domain_fr

                # Compile all domain values and domain lookup table, separately.
                if domain_en is None:
                    domains[table][field] = {"values": None, "lookup": None}

                elif isinstance(domain_en, list):
                    domains[table][field] = {
                        "values": sorted(list({*domain_en, *domain_fr}), reverse=True),
                        "lookup": dict([*zip(domain_en, domain_mapped), *zip(domain_fr, domain_mapped)])
                    }

                elif isinstance(domain_en, dict):
                    domains[table][field] = {
                        "values": sorted(list({*domain_en.values(), *domain_fr.values()}), reverse=True),
                        "lookup": {**domain_mapped,
                                   **{v: v for v in domain_mapped.values()},
                                   **{v: domain_mapped[k] for k, v in domain_non_mapped.items()}}
                    }

                    # Add integer keys as floats to accommodate incorrectly casted data.
                    for k, v in domain_mapped.items():
                        try:
                            domains[table][field]["lookup"].update({str(float(k)): v})
                        except ValueError:
                            continue

                else:
                    raise TypeError

            except (AttributeError, KeyError, TypeError, ValueError):
                yaml_paths = ", ".join(str(field_domains_path[lang]) for lang in ("en", "fr"))
                logger.exception(f"Unable to compile domains from config yamls: {yaml_paths}. Invalid schema "
                                 f"definition for table: {table}, field: {field}.")
                sys.exit(1)

    return domains


def compile_dtypes(length: bool = False) -> dict:
    """
    Compiles the dtype for each field in each NRN dataset. Optionally includes the field length.

    :param bool length: includes the length of the field in the returned data.
    :return dict: dictionary of dtypes and, optionally, length for each attribute of each NRN dataset.
    """

    dist_format = load_yaml(distribution_format_path)
    dtypes = dict()

    try:

        # Iterate tables.
        for name in dist_format:
            dtypes[name] = dict()

            # Iterate fields.
            for field, dtype in dist_format[name]["fields"].items():

                # Compile dtype and field length.
                dtypes[name][field] = dtype if length else dtype[0]

    except (AttributeError, KeyError, ValueError):
        logger.exception(f"Invalid schema definition: {dist_format}.")
        sys.exit(1)

    return dtypes


def explode_geometry(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Explodes MultiLineStrings and MultiPoints to LineStrings and Points, respectively.

    :param gpd.GeoDataFrame gdf: GeoDataFrame.
    :return gpd.GeoDataFrame: GeoDataFrame containing only single-part geometries.
    """

    logger.info("Exploding multi-type geometries.")

    multi_types = {"MultiLineString", "MultiPoint"}
    if len(set(gdf.geom_type.unique()).intersection(multi_types)):

        # Separate multi- and single-type records.
        multi = gdf.loc[gdf.geom_type.isin(multi_types)]
        single = gdf.loc[~gdf.index.isin(multi.index)]

        # Explode multi-type geometries.
        multi_exploded = multi.explode().reset_index(drop=True)

        # Merge all records.
        merged = gpd.GeoDataFrame(pd.concat([single, multi_exploded], ignore_index=True), crs=gdf.crs)
        return merged.copy(deep=True)

    else:
        return gdf.copy(deep=True)


def export(dataframes: Dict[str, Union[gpd.GeoDataFrame, pd.DataFrame]], output_path: Union[Path, str],
           driver: str = "GPKG", type_schemas: Union[None, dict, Path, str] = None,
           export_schemas: Union[None, dict, Path, str] = None, merge_schemas: bool = False,
           nln_map: Union[Dict[str, str], None] = None, keep_uuid: bool = True,
           outer_pbar: Union[tqdm, trange, None] = None, epsg: Union[None, str] = None,
           geom_type: Union[Dict[str, str], None] = None) -> None:
    """
    Exports one or more (Geo)DataFrames as a specified OGR driver file / layer.

    :param Dict[str, Union[gpd.GeoDataFrame, pd.DataFrame]] dataframes: dictionary of NRN dataset names and associated
        (Geo)DataFrames.
    :param Union[Path, str] output_path: output path.
    :param str driver: OGR driver short name, default 'GPKG'.
    :param Union[None, dict, Path, str] type_schemas: optional dictionary mapping of field types and widths for each
        provided dataset. Can also be a Path or str path to a pre-existing yaml. Expected dictionary format:
        {
            <dataset_name>:
                spatial: <bool>
                fields:
                    <field_name>: [<field_type>, <field_length>]
                    ...
                ...
        }
    :param Union[None, dict, Path, str] export_schemas: optional dictionary mapping of field names for each provided
        dataset. Can also be a Path or str path to a pre-existing yaml. Expected dictionary format:
        {
            conform:
                <dataset_name>:
                    fields:
                        <field_name>: <new_field_name>
                        ...
                ...
        }
    :param bool merge_schemas: optional flag to merge type and export schemas such that attributes from any dataset can
        exist on each provided dataset, default False.
    :param Union[Dict[str], None] nln_map: optional dictionary mapping of new layer names.
    :param bool keep_uuid: optional flag to preserve the uuid column, default True.
    :param Union[tqdm, trange, None] outer_pbar: optional pre-existing tqdm progress bar.
    :param Union[None, int] epsg: optional EPSG code used as the output CRS.
    :param Union[Dict[str, str], None] geom_type: optional dictionary mapping of Shapely geometry types used as the
        output geometry for the provided datasets. Must be one of 'Point', 'MultiPoint', 'LineString',
        'MultiLineString'.
    """

    try:

        # Validate / create driver.
        if driver not in {"ESRI Shapefile", "GML", "GPKG", "KML"}:
            raise ValueError("Invalid OGR driver, must be one of: ESRI Shapefile, GML, GPKG, KML.")
        driver = ogr.GetDriverByName(driver)

        # Create directory structure and data source (only create source for layer-based drivers).
        output_path = Path(output_path).resolve()

        if output_path.suffix:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            if output_path.exists():
                source = driver.Open(str(output_path), update=1)
            else:
                source = driver.CreateDataSource(str(output_path))
        else:
            output_path.mkdir(parents=True, exist_ok=True)
            source = None

        # Compile type schemas.
        if isinstance(type_schemas, (dict, Path, str)):
            if isinstance(type_schemas, (Path, str)):
                if Path(type_schemas).exists():
                    type_schemas = load_yaml(type_schemas)
                else:
                    raise ValueError(f"Invalid type schemas: {type_schemas}.")
        else:
            type_schemas = load_yaml(distribution_format_path)

        # Compile export schemas (filter datasets and fields within the existing type schemas and dataframe columns).
        if isinstance(export_schemas, (dict, Path, str)):
            if isinstance(export_schemas, (Path, str)):
                if Path(export_schemas).exists():
                    export_schemas, load_yaml(export_schemas)
                else:
                    raise ValueError(f"Invalid export schemas: {export_schemas}.")
            export_schemas = export_schemas["conform"]
        else:
            export_schemas = defaultdict(dict)
            for table, df in dataframes.items():
                export_schemas[table]["fields"] = {field: field for field in type_schemas[table]["fields"]}

        # Conditionally merge schemas.
        if merge_schemas:
            type_schemas_merged, export_schemas_merged = defaultdict(dict), defaultdict(dict)
            for table in dataframes:
                type_schemas_merged["fields"] |= type_schemas[table]["fields"]
                export_schemas_merged["fields"] |= export_schemas[table]["fields"]

            # Update schemas with merged results.
            type_schemas = {table: type_schemas_merged for table in type_schemas}
            export_schemas = {table: export_schemas_merged for table in export_schemas}

        # Iterate dataframes.
        for table, df in dataframes.items():

            # Configure layer shape type and spatial reference.
            spatial = type_schemas[table]["spatial"]
            if spatial:

                # Configure spatial reference.
                srs = osr.SpatialReference()
                if isinstance(epsg, int):
                    srs.ImportFromEPSG(epsg)
                else:
                    srs.ImportFromEPSG(df.crs.to_epsg())

                # Configure shape type.
                if isinstance(geom_type, dict):
                    try:
                        shape_type = attrgetter(f"wkb{geom_type[table]}")(ogr)
                    except KeyError:
                        raise KeyError(f"Invalid geom_type mapping: {geom_type}.")
                else:
                    if len(df.geom_type.unique()) > 1:
                        raise ValueError(f"Multiple geometry types detected for dataframe {table}: "
                                         f"{', '.join(map(str, df.geom_type.unique()))}.")
                    else:
                        shape_type = attrgetter(f"wkb{df.geom_type.iloc[0]}")(ogr)

            else:
                shape_type = ogr.wkbNone
                srs = None

            # Create source (non-layer-based drivers only) and layer.
            nln = str(nln_map[table]) if nln_map else table
            if driver.name == "GPKG":
                layer = source.CreateLayer(name=nln, srs=srs, geom_type=shape_type, options=["OVERWRITE=YES"])
            elif output_path.suffix:
                layer = source.CreateLayer(name=nln, srs=srs, geom_type=shape_type)
            else:
                source = driver.CreateDataSource(str(output_path / nln))
                layer = source.CreateLayer(name=Path(nln).stem, srs=srs, geom_type=shape_type)

            # Configure layer schema (field definitions).
            ogr_field_map = {"float": ogr.OFTReal, "int": ogr.OFTInteger, "str": ogr.OFTString}

            # Filter type and export schemas.
            type_schema = {field: specs for field, specs in type_schemas[table]["fields"].items() if field in df}
            valid_fields = set(type_schema).intersection(set(df.columns))
            export_schema = {field: map_field for field, map_field in export_schemas[table]["fields"].items()
                             if field in valid_fields}

            # Conditionally add uuid to schemas.
            if keep_uuid and "uuid" in df.columns:
                type_schema["uuid"] = ["str", 32]
                export_schema["uuid"] = "uuid"

            # Set field definitions from schemas.
            for field_name, mapped_field_name in export_schema.items():
                field_type, field_width = type_schema[field_name]
                field_defn = ogr.FieldDefn(mapped_field_name, ogr_field_map[field_type])
                field_defn.SetWidth(field_width)
                layer.CreateField(field_defn)

            # Remove invalid columns and reorder dataframe to match export schema.
            if spatial:
                df = df[[*export_schema, "geometry"]].copy(deep=True)
            else:
                df = df[[*export_schema]].copy(deep=True)

            # Map dataframe column names (does nothing if already mapped).
            df.rename(columns=export_schema, inplace=True)

            # Write layer.
            layer.StartTransaction()

            for feat in tqdm(df.itertuples(index=False), total=len(df),
                             desc=f"Writing to file={source.GetName()}, layer={table}",
                             bar_format="{desc}: |{bar}| {percentage:3.0f}% {r_bar}", leave=not bool(outer_pbar)):

                # Instantiate feature.
                feature = ogr.Feature(layer.GetLayerDefn())

                # Compile feature properties.
                properties = feat._asdict()

                # Set feature geometry, if spatial.
                if spatial:
                    geom = ogr.CreateGeometryFromWkb(properties.pop("geometry").wkb)
                    feature.SetGeometry(geom)

                # Iterate and set feature properties (attributes).
                for field_index, prop in enumerate(properties.items()):
                    feature.SetField(field_index, prop[-1])

                # Create feature.
                layer.CreateFeature(feature)

                # Clear pointer for next iteration.
                feature = None

            layer.CommitTransaction()

            # Update outer progress bar.
            if outer_pbar:
                outer_pbar.update(1)

    except FileExistsError as e:
        logger.exception(f"Invalid output directory - already exists.")
        logger.exception(e)
        sys.exit(1)
    except (Exception, KeyError, ValueError, sqlite3.Error) as e:
        logger.exception(f"Error raised when writing output: {output_path}.")
        logger.exception(e)
        sys.exit(1)


def flatten_coordinates(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Flattens the GeoDataFrame geometry coordinates to 2-dimensions.

    :param gpd.GeoDataFrame gdf: GeoDataFrame.
    :return gpd.GeoDataFrame: GeoDataFrame with 2-dimensional coordinates.
    """

    logger.info("Flattening coordinates to 2-dimensions.")

    # Flatten coordinates.
    if len(gdf.geom_type.unique()) > 1:
        raise Exception("Multiple geometry types detected for dataframe.")

    elif gdf.geom_type.iloc[0] == "LineString":
        gdf["geometry"] = gdf["geometry"].map(
            lambda g: LineString(itemgetter(0, 1)(pt) for pt in attrgetter("coords")(g)))

    elif gdf.geom_type.iloc[0] == "Point":
        gdf["geometry"] = gdf["geometry"].map(lambda g: Point(itemgetter(0, 1)(attrgetter("coords")(g)[0])))

    else:
        raise Exception("Geometry type not supported for coordinate flattening.")

    return gdf


def gdf_to_nx(gdf: gpd.GeoDataFrame, keep_attributes: bool = True, endpoints_only: bool = False) -> nx.Graph:
    """
    Converts a GeoDataFrame to a networkx Graph.

    :param gpd.GeoDataFrame gdf: GeoDataFrame.
    :param bool keep_attributes: keep the GeoDataFrame attributes on the networkx Graph, default True.
    :param bool endpoints_only: keep only the endpoints of the GeoDataFrame LineStrings, default False.
    :return nx.Graph: networkx Graph.
    """

    logger.info("Loading GeoPandas GeoDataFrame into NetworkX graph.")

    # Generate graph from GeoDataFrame of LineStrings, keeping crs property and (optionally) fields.
    g = nx.Graph()
    g.graph['crs'] = gdf.crs
    fields = list(gdf.columns) if keep_attributes else None

    # Iterate rows.
    for index, row in gdf.iterrows():

        # Compile geometry as edges.
        coords = [*row.geometry.coords]
        if endpoints_only:
            edges = [[coords[0], coords[-1]]]
        else:
            edges = [[coords[i], coords[i + 1]] for i in range(len(coords) - 1)]

        # Compile attributes.
        attributes = dict()
        if keep_attributes:
            data = [row[field] for field in fields]
            attributes = dict(zip(fields, data))

        # Add edges.
        g.add_edges_from(edges, **attributes)

    logger.info("Successfully loaded GeoPandas GeoDataFrame into NetworkX graph.")

    return g


def get_url(url: str, attempt: int = 1, **kwargs: dict) -> requests.Response:
    """
    Fetches a response from a url, using exponential backoff for failed attempts.

    :param str url: string url.
    :param int attempt: current count of attempts to get a response from the url.
    :param dict \*\*kwargs: keyword arguments passed to :func:`~requests.get`.
    :return requests.Response: response from the url.
    """

    logger.info(f"Fetching url request from: {url} [attempt {attempt}].")

    try:

        # Get url response.
        response = requests.get(url, **kwargs)

    except (TimeoutError, requests.exceptions.RequestException, requests.exceptions.ConnectionError) as e:
        logger.warning("Failed to get url response. Retrying with backoff...")
        logger.exception(e)

        # Retry with exponential backoff.
        time.sleep(2**attempt + random.random()*0.01)
        return get_url(url, attempt+1, **kwargs)

    return response


def groupby_to_list(df: Union[gpd.GeoDataFrame, pd.DataFrame], group_field: Union[List[str], str], list_field: str) -> \
        pd.Series:
    """
    Faster alternative to :func:`~pd.groupby.apply/agg(list)`.
    Groups records by one or more fields and compiles an output field into a list for each group.

    :param Union[gpd.GeoDataFrame, pd.DataFrame] df: (Geo)DataFrame.
    :param Union[List[str], str] group_field: field or list of fields by which the (Geo)DataFrame records will be
        grouped.
    :param str list_field: (Geo)DataFrame field to output, based on the record groupings.
    :return pd.Series: Series of grouped values.
    """

    if isinstance(group_field, list):
        for field in group_field:
            if df[field].dtype.name != "geometry":
                df[field] = df[field].astype("U")
        transpose = df.sort_values(group_field)[[*group_field, list_field]].values.T
        keys, vals = np.column_stack(transpose[:-1]), transpose[-1]
        keys_unique, keys_indexes = np.unique(keys.astype("U") if isinstance(keys, np.object) else keys,
                                              axis=0, return_index=True)

    else:
        keys, vals = df.sort_values(group_field)[[group_field, list_field]].values.T
        keys_unique, keys_indexes = np.unique(keys, return_index=True)

    vals_arrays = np.split(vals, keys_indexes[1:])

    return pd.Series([list(vals_array) for vals_array in vals_arrays], index=keys_unique).copy(deep=True)


def load_gpkg(gpkg_path: Union[Path, str], find: bool = False, layers: Union[None, List[str]] = None) -> \
        Dict[str, Union[gpd.GeoDataFrame, pd.DataFrame]]:
    """
    Compiles a dictionary of NRN dataset names and associated (Geo)DataFrame from GeoPackage layers.

    :param Union[Path, str] gpkg_path: path to the GeoPackage.
    :param bool find: searches for NRN datasets in the GeoPackage based on non-exact matches with the expected dataset
        names, default False.
    :param Union[None, List[str]] layers: layer name or list of layer names to return instead of all NRN datasets.
    :return Dict[str, Union[gpd.GeoDataFrame, pd.DataFrame]]: dictionary of NRN dataset names and associated
        (Geo)DataFrames.
    """

    logger.info(f"Loading GeoPackage: {gpkg_path}.")

    dframes = dict()
    distribution_format = load_yaml(distribution_format_path)
    gpkg_path = Path(gpkg_path).resolve()

    if gpkg_path.exists():

        # Filter layers to load.
        if layers:
            distribution_format = {k: v for k, v in distribution_format.items() if k in layers}

        try:

            # Create sqlite connection.
            con = sqlite3.connect(gpkg_path)
            cur = con.cursor()

            # Load GeoPackage table names.
            gpkg_layers = list(zip(*cur.execute("select name from sqlite_master where type='table';").fetchall()))[0]

            # Create table name mapping.
            layers_map = dict()
            if find:
                for table_name in distribution_format:
                    for layer_name in gpkg_layers:
                        if layer_name.lower().find(table_name) >= 0:
                            layers_map[table_name] = layer_name
                            break
            else:
                layers_map = {name: name for name in set(distribution_format).intersection(set(gpkg_layers))}

        except sqlite3.Error:
            logger.exception(f"Unable to connect to GeoPackage: {gpkg_path}.")
            sys.exit(1)

        # Compile missing layers.
        missing_layers = set(distribution_format) - set(layers_map)
        if missing_layers:
            logger.warning(f"Missing one or more expected layers: {', '.join(map(str, sorted(missing_layers)))}. An "
                           f"exception may be raised later on if any of these layers are required.")

        # Load GeoPackage layers as (geo)dataframes.
        # Convert column names to lowercase on import.
        for table_name in layers_map:

            logger.info(f"Loading layer: {table_name}.")

            try:

                # Spatial data.
                if distribution_format[table_name]["spatial"]:
                    df = gpd.read_file(gpkg_path, layer=layers_map[table_name], driver="GPKG").rename(columns=str.lower)

                # Tabular data.
                else:
                    df = pd.read_sql_query(f"select * from {layers_map[table_name]}", con).rename(columns=str.lower)

                # Set index field: uuid.
                if "uuid" in df.columns:
                    df.index = df["uuid"]

                # Drop fid field (this field is automatically generated and not part of the NRN).
                if "fid" in df.columns:
                    df.drop(columns=["fid"], inplace=True)

                # Fill nulls with string "None".
                df.fillna("None", inplace=True)

                # Store result.
                dframes[table_name] = df.copy(deep=True)
                logger.info(f"Successfully loaded layer as dataframe: {table_name}.")

            except (fiona.errors.DriverError, pd.io.sql.DatabaseError, sqlite3.Error):
                logger.exception(f"Unable to load layer: {table_name}.")
                sys.exit(1)

    else:
        logger.exception(f"GeoPackage does not exist: {gpkg_path}.")
        sys.exit(1)

    return dframes


def load_yaml(path: Union[Path, str]) -> Any:
    """
    Loads the content of a YAML file as a Python object.

    :param Union[Path, str] path: path to the YAML file.
    :return Any: Python object consisting of the YAML content.
    """

    path = Path(path).resolve()

    with open(path, "r", encoding="utf8") as f:

        try:

            return yaml.safe_load(f)

        except (ValueError, yaml.YAMLError):
            logger.exception(f"Unable to load yaml: {path}.")


def nx_to_gdf(g: nx.Graph, nodes: bool = True, edges: bool = True) -> \
        Union[gpd.GeoDataFrame, Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]]:
    """
    Converts a networkx Graph to a GeoDataFrame.

    :param nx.Graph g: networkx Graph.
    :param bool nodes: return a Point GeoDataFrame, derived from the network Graph nodes, default True.
    :param bool edges: return a LineString GeoDataFrame, derived from the network Graph edges, default True.
    :return Union[gpd.GeoDataFrame, Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]]: a Point GeoDataFrame and / or LineString
        GeoDataFrame, derived from the networkx Graph nodes and / or edges, respectively.
    """

    logger.info("Loading NetworkX graph into GeoPandas GeoDataFrame.")

    # Generate GeoDataFrames for both networkx nodes and edges.
    gdf_nodes, gdf_edges = None, None

    # Compile node geometry and attributes.
    if nodes:
        node_xy, node_data = zip(*g.nodes(data=True))
        gdf_nodes = gpd.GeoDataFrame(list(node_data), geometry=[Point(i, j) for i, j in node_xy])
        gdf_nodes.crs = g.graph['crs']

    # Compile edge geometry and attributes.
    if edges:
        starts, ends, edge_data = zip(*g.edges(data=True))
        gdf_edges = gpd.GeoDataFrame(list(edge_data))
        gdf_edges.crs = g.graph['crs']

    logger.info("Successfully loaded GeoPandas GeoDataFrame into NetworkX graph.")

    # Conditionally return nodes and / or edges.
    if all([nodes, edges]):
        return gdf_nodes, gdf_edges
    elif nodes is True:
        return gdf_nodes
    else:
        return gdf_edges


def rm_tree(path: Path) -> None:
    """
    Recursively removes a directory and all of its contents.

    :param Path path: path to the directory to be removed.
    """

    if path.exists():

        # Recursively remove directory contents.
        for child in path.iterdir():
            if child.is_file():
                child.unlink()
            else:
                rm_tree(child)

        # Remove original directory.
        path.rmdir()

    else:
        logger.exception(f"Path does not exist: \"{path}\".")
        sys.exit(1)


def round_coordinates(gdf: gpd.GeoDataFrame, precision: int = 7) -> gpd.GeoDataFrame:
    """
    Rounds the GeoDataFrame geometry coordinates to a specific decimal precision.

    :param gpd.GeoDataFrame gdf: GeoDataFrame.
    :param int precision: decimal precision to round the GeoDataFrame geometry coordinates to.
    :return gpd.GeoDataFrame: GeoDataFrame with modified decimal precision.
    """

    logger.info(f"Rounding coordinates to decimal precision: {precision}.")

    try:

        gdf["geometry"] = gdf["geometry"].map(
            lambda g: loads(re.sub(r"\d*\.\d+", lambda m: f"{float(m.group(0)):.{precision}f}", g.wkt)))

        return gdf

    except (TypeError, ValueError) as e:
        logger.exception("Unable to round coordinates for GeoDataFrame.")
        logger.exception(e)
        sys.exit(1)
