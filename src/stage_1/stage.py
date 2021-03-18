import click
import fiona
import geopandas as gpd
import logging
import numpy as np
import pandas as pd
import re
import requests
import shutil
import sys
import uuid
import zipfile
from collections import Counter
from copy import deepcopy
from datetime import datetime
from operator import itemgetter
from pathlib import Path
from typing import Any, List, Type, Union

filepath = Path(__file__).resolve()
sys.path.insert(1, str(filepath.parents[1]))
import field_map_functions
import helpers
from segment_addresses import Segmentor


# Set logger.
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S"))
logger.addHandler(handler)


class Stage:
    """Defines an NRN stage."""

    def __init__(self, source: str, remove: bool = False, exclude_old: bool = False) -> None:
        """
        Initializes an NRN stage.

        :param str source: abbreviation for the source province / territory.
        :param bool remove: removes pre-existing files within the data/interim directory for the specified source,
            default False.
        :param bool exclude_old: excludes the previous NRN vintage for the specified source from being removed if
            remove=True, default False. Option has no effect if remove=False.
        """

        self.stage = 1
        self.source = source.lower()
        self.remove = remove
        self.exclude_old = exclude_old

        # Configure raw data path.
        self.data_path = filepath.parents[2] / f"data/raw/{self.source}"

        # Configure attribute paths.
        self.source_attribute_path = filepath.parent / f"sources/{self.source}"
        self.source_attributes = dict()
        self.target_attributes = dict()

        # Configure DataFrame collections.
        self.source_gdframes = dict()
        self.target_gdframes = dict()

        # Configure previous NRN vintage path and clear namespace.
        self.nrn_old_path = {ext: filepath.parents[2] / f"data/interim/{self.source}_old.{ext}"
                             for ext in ("gpkg", "zip")}

        # Configure output path.
        self.output_path = filepath.parents[2] / f"data/interim/{self.source}.gpkg"

        # Conditionally clear output namespace.
        namespace = list(filter(Path.is_file, self.output_path.parent.glob(f"{self.source}[_.]*")))

        if len(namespace):
            logger.warning("Output namespace already occupied.")

            if self.remove:
                logger.warning("Parameter remove=True: Removing conflicting files.")

                for f in namespace:

                    # Conditionally exclude previous NRN vintage.
                    if self.exclude_old and f.name == self.nrn_old_path["gpkg"].name:
                        logger.info(f"Parameter exclude-old=True: Excluding conflicting file from removal: \"{f}\".")
                        continue

                    # Remove files.
                    logger.info(f"Removing conflicting file: \"{f}\".")
                    f.unlink()

            else:
                logger.exception("Parameter remove=False: Unable to proceed while output namespace is occupied. Set "
                                 "remove=True (-r) or manually clear the output namespace.")
                sys.exit(1)

        # Configure field defaults, dtypes, and domains.
        self.defaults = helpers.compile_default_values()
        self.dtypes = helpers.compile_dtypes()
        self.domains = helpers.compile_domains()

    def apply_domains(self) -> None:
        """Applies domain restrictions to each column in the target (Geo)DataFrames."""

        def cast_dtype(val: Any, dtype: Type, default: Any) -> Any:
            """
            Casts the value to the given numpy dtype.
            Returns the default parameter for invalid or Null values.

            :param Any val: value.
            :param Type dtype: numpy type object to be casted to.
            :param Any default: value to be returned in case of error.
            :return Any: casted or default value.
            """

            try:

                if pd.isna(val) or val == "":
                    return default
                else:
                    return itemgetter(0)(np.array([val]).astype(dtype))

            except (TypeError, ValueError):
                return default

        logging.info("Applying field domains.")
        table = None
        field = None

        try:

            for table in self.target_gdframes:
                for field, domain in self.domains[table].items():

                    logger.info(f"Applying domain to table: {table}, field: {field}.")

                    # Copy series as object dtype.
                    series_orig = self.target_gdframes[table][field].copy(deep=True).astype(object)

                    # Apply domain to series.
                    series_new = helpers.apply_domain(series_orig, domain=domain["lookup"],
                                                      default=self.defaults[table][field])

                    # Compile original and new dtype names (for logging).
                    dtype_orig = self.target_gdframes[table][field].dtype.name
                    dtype_new = self.dtypes[table][field]

                    # Force adjust data type.
                    series_new = series_new.map(lambda val: cast_dtype(val, dtype_new, self.defaults[table][field]))

                    # Store results to target dataframe.
                    self.target_gdframes[table][field] = series_new.copy(deep=True)

                    # Compile and log modifications.
                    mods = series_orig.astype(str) != series_new.astype(str)
                    if mods.any():

                        # Compile and quantify modifications.
                        df = pd.DataFrame({"orig": series_orig[mods], "new": series_new[mods]})
                        counts = Counter(series_orig[mods].fillna(-99))

                        # Iterate and log record modifications.
                        for vals in df.loc[~df.duplicated(keep="first")].values:

                            logger.warning(f"Modified {counts[-99] if pd.isna(vals[0]) else counts[vals[0]]} "
                                           f"instance(s) of {vals[0]} ({dtype_orig}) to {vals[1]} ({dtype_new}).")

        except (AttributeError, KeyError, ValueError):
            logger.exception(f"Invalid schema definition for table: {table}, field: {field}.")
            sys.exit(1)

    def apply_field_mapping(self) -> None:
        """Maps the source (Geo)DataFrames to the target (Geo)DataFrames via user-specific field mapping functions."""

        logger.info("Applying field mapping.")

        # Retrieve source attributes and dataframe.
        for source_name, source_attributes in self.source_attributes.items():
            source_gdf = self.source_gdframes[source_name]

            # Retrieve target attributes.
            for target_name in source_attributes["conform"]:
                logger.info(f"Applying field mapping from {source_name} to {target_name}.")

                # Retrieve table field mapping attributes.
                maps = source_attributes["conform"][target_name]

                # Field mapping.
                for target_field, source_field in maps.items():

                    # Retrieve target dataframe.
                    target_gdf = self.target_gdframes[target_name]

                    # No mapping.
                    if source_field is None:
                        logger.info(f"Target field {target_field}: No mapping provided.")

                    # Raw value mapping.
                    elif isinstance(source_field, (str, int, float)) and str(source_field).lower() not in \
                            source_gdf.columns:
                        logger.info(f"Target field {target_field}: Applying raw value.")

                        # Update target dataframe with raw value.
                        target_gdf[target_field] = source_field

                    # Function mapping.
                    else:
                        logger.info(f"Target field {target_field}: Identifying function chain.")

                        # Restructure mapping dict for direct field mapping in case of string or list input.
                        if isinstance(source_field, (str, list)):
                            source_field = {
                                "fields": source_field if isinstance(source_field, list) else [source_field],
                                "functions": [{"function": "direct"}]
                            }

                        # Convert fields to lowercase.
                        if isinstance(source_field["fields"], list):
                            source_field["fields"] = list(map(str.lower, source_field["fields"]))
                        else:
                            source_field["fields"] = list(map(str.lower, [source_field["fields"]]))

                        # Create mapped dataframe from source and target dataframes, keeping only the source fields.
                        mapped_df = pd.DataFrame({field: target_gdf["uuid"].map(
                            source_gdf.set_index("uuid", drop=False)[field]) for field in source_field["fields"]})

                        # Determine if source fields must be processed separately or together.
                        try:
                            process_separately = itemgetter("process_separately")(source_field)
                        except KeyError:
                            process_separately = False

                        # Create dataframe to hold results if multiple fields are given and not processed separately.
                        if not process_separately or len(source_field["fields"]) == 1:
                            results = pd.Series()
                        else:
                            results = pd.DataFrame(columns=range(len(source_field["fields"])))

                        # Iterate source fields.
                        for index, field in enumerate(source_field["fields"]):

                            # Retrieve series from mapped dataframe.
                            if process_separately or len(source_field["fields"]) == 1:
                                mapped_series = mapped_df[field]
                            else:
                                mapped_series = mapped_df.apply(lambda row: row.values, axis=1)

                            # Apply field mapping functions to mapped series.
                            field_mapping_results = self.apply_functions(mapped_series, source_field["functions"],
                                                                         target_field)

                            # Store results.
                            if isinstance(results, pd.Series):
                                results = field_mapping_results.copy(deep=True)
                                break
                            else:
                                results[index] = field_mapping_results.copy(deep=True)

                        # Convert results dataframe to series, if required.
                        if isinstance(results, pd.Series):
                            field_mapping_results = results.copy(deep=True)
                        else:
                            field_mapping_results = results.apply(lambda row: row.values, axis=1)

                        # Update target dataframe.
                        target_gdf[target_field] = field_mapping_results.copy(deep=True)

                    # Store updated target dataframe.
                    self.target_gdframes[target_name] = target_gdf.copy(deep=True)

    def apply_functions(self, series: pd.Series, func_list: List[dict], target_field: str) -> pd.Series:
        """
        Iterates and applies field mapping function(s) to a Series.

        :param pd.Series series: Series.
        :param List[dict] func_list: list of yaml-constructed field mapping definitions passed to
            :func:`field_map_functions`.
        :param str target_field: name of the destination field to which the given Series will be assigned.
        :return pd.Series: mapped Series.
        """

        # Iterate functions.
        for func in func_list:
            func_name = func["function"]
            params = {k: v for k, v in func.items() if k not in {"function", "iterate_cols"}}

            logger.info(f"Target field {target_field}: Applying field mapping function: {func_name}.")

            # Generate expression.
            expr = f"field_map_functions.{func_name}(series, **params)"

            try:

                # Iterate nested columns.
                if "iterate_cols" in func and isinstance(series.iloc[0], list):

                    # Unpack nested Series as DataFrame and iterate required columns.
                    df = pd.DataFrame(series.tolist(), index=series.index)
                    for col_index in func["iterate_cols"]:

                        # Execute expression against individual Series.
                        series = df[col_index].copy(deep=True)
                        df[col_index] = eval(expr).copy(deep=True)

                    # Reconstruct nested Series.
                    series = df.apply(lambda row: row.values, axis=1)

                else:

                    # Execute expression.
                    series = eval(expr).copy(deep=True)

            except (IndexError, SyntaxError, ValueError):
                logger.exception(f"Invalid expression: {expr}.")
                sys.exit(1)

        return series

    def clean_datasets(self) -> None:
        """Applies a series of data cleanups to certain datasets."""

        logger.info(f"Applying data cleanup functions.")

        def enforce_accuracy_limits(table: str, df: Union[gpd.GeoDataFrame, pd.DataFrame]) -> \
                Union[gpd.GeoDataFrame, pd.DataFrame]:
            """
            Enforces upper and lower limits for NRN attribute 'accuracy'.

            :param str table: name of an NRN dataset.
            :param Union[gpd.GeoDataFrame, pd.DataFrame] df: (Geo)DataFrame containing the target NRN attribute(s).
            :return Union[gpd.GeoDataFrame, pd.DataFrame]: (Geo)DataFrame with attribute modifications.
            """

            logger.info(f"Applying data cleanup \"enforce accuracy limits\" to dataset: {table}.")

            # Enforce accuracy limits.
            series_orig = df["accuracy"].copy(deep=True)
            df.loc[df["accuracy"].between(-1, 1, inclusive=False), "accuracy"] = self.defaults[table]["accuracy"]

            # Quantify and log modifications.
            mods = (series_orig != df["accuracy"]).sum()
            if mods:
                logger.warning(f"Modified {mods} record(s) in table {table}, column: accuracy."
                               f"\nModification details: Accuracy set to default value for values between -1 and 1, "
                               f"exclusively.")

            return df.copy(deep=True)

        def lower_case_ids(table: str, df: Union[gpd.GeoDataFrame, pd.DataFrame]) -> \
                Union[gpd.GeoDataFrame, pd.DataFrame]:
            """
            Sets all ID fields to lower case.

            :param str table: name of an NRN dataset.
            :param Union[gpd.GeoDataFrame, pd.DataFrame] df: (Geo)DataFrame containing the target NRN attribute(s).
            :return Union[gpd.GeoDataFrame, pd.DataFrame]: (Geo)DataFrame with attribute modifications.
            """

            logger.info(f"Applying data cleanup \"lower case IDs\" to dataset: {table}.")

            # Iterate columns which a) end with "id", b) are str type, and c) are not uuid.
            dtypes = self.dtypes[table]
            for col in [fld for fld in df.columns.difference(["uuid"]) if fld.endswith("id") and dtypes[fld] == "str"]:

                # Filter records to non-default values which are not already lower case.
                default = self.defaults[table][col]
                s_filtered = df.loc[df[col].map(lambda val: val != default and not val.islower()), col]

                # Apply modifications, if required.
                if len(s_filtered):
                    df.loc[s_filtered.index, col] = s_filtered.map(str.lower)

                    # Quantify and log modifications.
                    logger.warning(f"Modified {len(s_filtered)} record(s) in table {table}, column {col}."
                                   "\nModification details: Column values set to lower case.")

            return df.copy(deep=True)

        def overwrite_segment_ids(table: str, df: Union[gpd.GeoDataFrame, pd.DataFrame]) -> \
                Union[gpd.GeoDataFrame, pd.DataFrame]:
            """
            Populates the NRN attributes 'ferrysegid' or 'roadsegid', whichever appropriate, with incrementing integer
            values from 1-n.

            :param str table: name of an NRN dataset.
            :param Union[gpd.GeoDataFrame, pd.DataFrame] df: (Geo)DataFrame containing the target NRN attribute(s).
            :return Union[gpd.GeoDataFrame, pd.DataFrame]: (Geo)DataFrame with attribute modifications.
            """

            if table in {"ferryseg", "roadseg"}:

                logger.info(f"Applying data cleanup \"overwrite segment IDs\" to dataset: {table}.")

                # Overwrite column.
                col = {"ferryseg": "ferrysegid", "roadseg": "roadsegid"}[table]
                df[col] = range(1, len(df) + 1)

            return df.copy(deep=True)

        def standardize_nones(table: str, df: Union[gpd.GeoDataFrame, pd.DataFrame]) -> \
                Union[gpd.GeoDataFrame, pd.DataFrame]:
            """
            Standardizes string 'None's (distinct from Null).

            :param str table: name of an NRN dataset.
            :param Union[gpd.GeoDataFrame, pd.DataFrame] df: (Geo)DataFrame containing the target NRN attribute(s).
            :return Union[gpd.GeoDataFrame, pd.DataFrame]: (Geo)DataFrame with attribute modifications.
            """

            logger.info(f"Applying data cleanup \"standardize_nones\" to dataset: {table}.")

            # Compile valid columns.
            cols = df.select_dtypes(include="object", exclude="geometry").columns.values

            # Iterate columns.
            for col in cols:

                # Apply modifications.
                series_orig = df[col].copy(deep=True)
                df.loc[df[col].map(str.lower) == "none", col] = "None"

                # Quantify and log modifications.
                mods = (series_orig != df[col]).sum()
                if mods:
                    logger.warning(f"Modified {mods} record(s) in table {table}, column {col}."
                                   f"\nModification details: Column values standardized to \"None\".")

            return df.copy(deep=True)

        def strip_whitespace(table: str, df: Union[gpd.GeoDataFrame, pd.DataFrame]) -> \
                Union[gpd.GeoDataFrame, pd.DataFrame]:
            """
            Strips leading, trailing, and multiple internal whitespace for each (Geo)DataFrame column.

            :param str table: name of an NRN dataset.
            :param Union[gpd.GeoDataFrame, pd.DataFrame] df: (Geo)DataFrame containing the target NRN attribute(s).
            :return Union[gpd.GeoDataFrame, pd.DataFrame]: (Geo)DataFrame with attribute modifications.
            """

            logger.info(f"Applying data cleanup \"strip whitespace\" to dataset: {table}.")

            # Compile valid columns.
            cols = df.select_dtypes(include="object", exclude="geometry").columns.values

            # Iterate columns.
            for col in cols:

                # Apply modifications.
                series_orig = df[col].copy(deep=True)
                df[col] = df[col].map(lambda val: re.sub(r" +", " ", str(val.strip())))

                # Quantify and log modifications.
                mods = (series_orig != df[col]).sum()
                if mods:
                    logger.warning(f"Modified {mods} record(s) in table {table}, column {col}."
                                   "\nModification details: Column values stripped of leading, trailing, and multiple "
                                   "internal whitespace.")

            return df.copy(deep=True)

        def title_case_names(table: str, df: Union[gpd.GeoDataFrame, pd.DataFrame]) -> \
                Union[gpd.GeoDataFrame, pd.DataFrame]:
            """
            Sets to title case all NRN name attributes:
                ferryseg: rtename1en, rtename1fr, rtename2en, rtename2fr, rtename3en, rtename3fr, rtename4en, rtename4fr
                roadseg: l_placenam, l_stname_c, r_placenam, r_stname_c, rtename1en, rtename1fr, rtename2en, rtename2fr,
                         rtename3en, rtename3fr, rtename4en, rtename4fr, strunameen, strunamefr
                strplaname: namebody, placename

            :param str table: name of an NRN dataset.
            :param Union[gpd.GeoDataFrame, pd.DataFrame] df: (Geo)DataFrame containing the target NRN attribute(s).
            :return Union[gpd.GeoDataFrame, pd.DataFrame]: (Geo)DataFrame with attribute modifications.
            """

            if table in {"ferryseg", "roadseg", "strplaname"}:

                logger.info(f"Applying data cleanup \"title case names\" to dataset: {table}.")

                # Define name fields.
                name_fields = {
                    "ferryseg": ["rtename1en", "rtename1fr", "rtename2en", "rtename2fr", "rtename3en", "rtename3fr",
                                 "rtename4en", "rtename4fr"],
                    "roadseg": ["l_placenam", "l_stname_c", "r_placenam", "r_stname_c", "rtename1en", "rtename1fr",
                                "rtename2en", "rtename2fr", "rtename3en", "rtename3fr", "rtename4en", "rtename4fr",
                                "strunameen", "strunamefr"],
                    "strplaname": ["namebody", "placename"]
                }

                # Iterate columns.
                for col in name_fields[table]:

                    # Filter records to non-default values which are not already title case.
                    default = self.defaults[table][col]
                    s_filtered = df.loc[df[col].map(lambda route: route != default and not route.istitle()), col]

                    # Apply modifications, if required.
                    if len(s_filtered):
                        df.loc[s_filtered.index, col] = s_filtered.map(str.title)

                        # Quantify and log modifications.
                        logger.warning(f"Modified {len(s_filtered)} record(s) in table {table}, column {col}."
                                       "\nModification details: Column values set to title case.")

            return df.copy(deep=True)

        # Apply cleanup functions.
        for table, df in self.target_gdframes.items():

            # Iterate cleanup functions.
            for func in (lower_case_ids, strip_whitespace, standardize_nones, overwrite_segment_ids, title_case_names,
                         enforce_accuracy_limits):
                df = func(table, df)

            # Store updated dataframe.
            self.target_gdframes.update({table: df.copy(deep=True)})

    def compile_source_attributes(self) -> None:
        """Compiles the yaml files in the sources' directory into a dictionary."""

        logger.info("Compiling source attribute yamls.")
        self.source_attributes = dict()

        # Iterate source yamls.
        for f in filter(Path.is_file, Path(self.source_attribute_path).glob("*.yaml")):

            # Load yaml and store contents.
            self.source_attributes[f.stem] = helpers.load_yaml(f)

    def compile_target_attributes(self) -> None:
        """Compiles the yaml file for the target (Geo)DataFrames (distribution format) into a dictionary."""

        logger.info("Compiling target attribute yaml.")
        table = field = None

        # Load yaml.
        self.target_attributes = helpers.load_yaml(filepath.parents[1] / "distribution_format.yaml")

        # Remove field length from dtype attribute.
        logger.info("Configuring target attributes.")
        try:

            for table in self.target_attributes:
                for field, vals in self.target_attributes[table]["fields"].items():
                    self.target_attributes[table]["fields"][field] = vals[0]

        except (AttributeError, KeyError, ValueError):
            logger.exception(f"Invalid schema definition for table: {table}, field: {field}.")
            sys.exit(1)

    def download_previous_vintage(self) -> None:
        """Downloads the previous NRN vintage and extracts the English GeoPackage as <source>_old.gpkg."""

        logger.info("Retrieving previous NRN vintage.")

        # Determine download requirement.
        if self.nrn_old_path["gpkg"].exists():
            logger.warning(f"Previous NRN vintage already exists: \"{self.nrn_old_path['gpkg']}\". Skipping step.")

        else:

            # Download previous NRN vintage.
            logger.info("Downloading previous NRN vintage.")
            download_url = None

            try:

                # Get download url.
                download_url = helpers.load_yaml(
                    filepath.parents[1] / "downloads.yaml")["previous_nrn_vintage"][self.source]

                # Get raw content stream from download url.
                download = helpers.get_url(download_url, stream=True, timeout=30, verify=False)

                # Copy download content to file.
                with open(self.nrn_old_path["zip"], "wb") as f:
                    shutil.copyfileobj(download.raw, f)

            except (requests.exceptions.RequestException, shutil.Error) as e:
                logger.exception(f"Unable to download previous NRN vintage: \"{download_url}\".")
                logger.exception(e)
                sys.exit(1)

            # Extract zipped data.
            logger.info("Extracting zipped data for previous NRN vintage.")

            gpkg_download = [f for f in zipfile.ZipFile(self.nrn_old_path["zip"], "r").namelist() if
                             f.lower().startswith("nrn") and Path(f).suffix == ".gpkg"][0]

            with zipfile.ZipFile(self.nrn_old_path["zip"], "r") as zip_f:
                with zip_f.open(gpkg_download) as zsrc, open(self.nrn_old_path["gpkg"], "wb") as zdest:
                    shutil.copyfileobj(zsrc, zdest)

            # Remove temporary files.
            logger.info("Removing temporary files for previous NRN vintage.")

            if self.nrn_old_path["zip"].exists():
                self.nrn_old_path["zip"].unlink()

    def filter_and_relink_strplaname(self) -> None:
        """Reduces duplicated records, where possible, in NRN strplaname and repairs the remaining NID linkages."""

        df = self.target_gdframes["strplaname"].copy(deep=True)

        # Filter duplicates.
        logger.info("Filtering duplicates from strplaname.")

        # Define match fields and drop duplicates.
        match_fields = list(df.columns.difference(["uuid", "nid"]))
        df_new = df.drop_duplicates(subset=match_fields, keep="first", inplace=False)

        if len(df) != len(df_new):

            # Store results.
            self.target_gdframes["strplaname"] = df_new.copy(deep=True)

            # Quantify removed duplicates.
            logger.info(f"Dropped {len(df) - len(df_new)} duplicated records from strplaname.")

            # Repair nid linkages.
            logger.info("Repairing strplaname.nid linkages.")

            # Define nid linkages.
            linkages = {
                "addrange": ["l_offnanid", "r_offnanid"],
                "altnamlink": ["strnamenid"]
            }

            # Generate nid lookup dict.
            # Process: group nids by match fields, set first value in each group as index, explode groups, create dict
            # from reversed index and values.
            nids_grouped = helpers.groupby_to_list(df, match_fields, "nid")
            nids_grouped.index = nids_grouped.map(itemgetter(0))
            nids_exploded = nids_grouped.explode()
            nid_lookup = dict(zip(nids_exploded.values, nids_exploded.index))

            # Iterate nid linkages.
            for table in set(linkages).intersection(set(self.target_gdframes)):
                for field in linkages[table]:

                    # Repair nid linkage.
                    series = self.target_gdframes[table][field].copy(deep=True)
                    self.target_gdframes[table].loc[series.index, field] = series.map(
                        lambda val: itemgetter(val)(nid_lookup))

                    # Quantify and log modifications.
                    mods_count = (series != self.target_gdframes[table][field]).sum()
                    if mods_count:
                        logger.warning(f"Repaired {mods_count} linkage(s) between strplaname.nid - {table}.{field}.")

    def gen_source_dataframes(self) -> None:
        """
        Loads raw source data into GeoDataFrames and applies a series of standardizations, most notably:
        1) explode multi-type geometries.
        2) reprojection to NRN standard EPSG:4617.
        3) round coordinate precision to NRN standard 7 decimal places.
        """

        logger.info("Loading source data as dataframes.")
        self.source_gdframes = dict()

        for source, source_yaml in self.source_attributes.items():

            logger.info(f"Loading source data for {source}.yaml: file={source_yaml['data']['filename']}, layer="
                        f"{source_yaml['data']['layer']}.")

            # Load source data into a geodataframe.
            try:

                df = gpd.read_file(self.data_path / source_yaml["data"]["filename"],
                                   driver=source_yaml["data"]["driver"],
                                   layer=source_yaml["data"]["layer"])

            except fiona.errors.FionaValueError as e:
                logger.exception(f"Unable to load data source.")
                logger.exception(e)
                sys.exit(1)

            # Query dataframe.
            if source_yaml["data"]["query"]:
                try:
                    df.query(source_yaml["data"]["query"], inplace=True)
                except ValueError as e:
                    logger.exception(f"Invalid query: \"{source_yaml['data']['query']}\".")
                    logger.exception(e)
                    sys.exit(1)

            # Force lowercase column names.
            df.columns = map(str.lower, df.columns)

            # Apply spatial data modifications.
            if source_yaml["data"]["spatial"]:

                # Filter invalid geometries.
                df = df.loc[df.geom_type.isin({"Point", "MultiPoint", "LineString", "MultiLineString"})]

                # Cast multi-type geometries.
                df = helpers.explode_geometry(df)

                # Reproject to EPSG:4617.
                df = helpers.reproject_gdf(df, int(source_yaml["data"]["crs"].split(":")[-1]), 4617)

                # Force coordinates to 2D.
                df = helpers.flatten_coordinates(df)

                # Round coordinates to decimal precision = 7.
                df = helpers.round_coordinates(df, 7)

            # Add uuid field.
            df["uuid"] = [uuid.uuid4().hex for _ in range(len(df))]

            # Store result.
            self.source_gdframes[source] = df.copy(deep=True)

            logger.info("Successfully loaded source data.")

    def gen_target_dataframes(self) -> None:
        """Creates empty (Geo)DataFrames for all applicable output tables."""

        logger.info("Creating target dataframes for applicable tables.")
        self.target_gdframes = dict()

        # Retrieve target table names from source attributes.
        for source, source_yaml in self.source_attributes.items():
            for table in source_yaml["conform"]:

                logger.info(f"Creating target dataframe: {table}.")

                # Spatial.
                if self.target_attributes[table]["spatial"]:

                    # Generate target dataframe from source uuid and geometry fields.
                    gdf = gpd.GeoDataFrame(self.source_gdframes[source][["uuid"]],
                                           geometry=self.source_gdframes[source].geometry,
                                           crs="EPSG:4617")

                # Tabular.
                else:

                    # Generate target dataframe from source uuid field.
                    gdf = pd.DataFrame(self.source_gdframes[source][["uuid"]])

                # Add target field schema.
                gdf = gdf.assign(**{field: pd.Series(dtype=dtype) for field, dtype in
                                    self.target_attributes[table]["fields"].items()})

                # Store result.
                self.target_gdframes[table] = gdf
                logger.info(f"Successfully created target dataframe: {table}.")

        # Log unavailable datasets.
        for table in [t for t in self.target_attributes if t not in self.target_gdframes]:

            logger.warning(f"Source data provides no field mappings for table: {table}.")

    def recover_missing_datasets(self) -> None:
        """
        Recovers missing NRN datasets in the current vintage from the previous vintage.
        Exception: altnamlink, junction.
        """

        # Identify datasets to be recovered.
        recovery_tables = set(self.target_attributes) - set(self.target_gdframes) - {"altnamlink", "junction"}
        if recovery_tables:

            logger.info("Recovering missing datasets from the previous NRN vintage.")

            # Iterate datasets from previous NRN vintage.
            for table, df in helpers.load_gpkg(self.nrn_old_path["gpkg"], find=True, layers=recovery_tables).items():

                # Recover non-empty datasets.
                if len(df):

                    logger.info(f"Recovering dataset: {table}.")

                    # Add uuid field.
                    df["uuid"] = [uuid.uuid4().hex for _ in range(len(df))]

                    if isinstance(df, gpd.GeoDataFrame):

                        # Filter invalid geometries.
                        df = df.loc[df.geom_type.isin({"Point", "MultiPoint", "LineString", "MultiLineString"})]

                        # Cast multi-type geometries.
                        df = helpers.explode_geometry(df)

                        # Reproject to EPSG:4617.
                        df = helpers.reproject_gdf(df, df.crs.to_epsg(), 4617)

                        # Force coordinates to 2D.
                        df = helpers.flatten_coordinates(df)

                        # Round coordinates to decimal precision = 7.
                        df = helpers.round_coordinates(df, precision=7)

                    # Store result.
                    self.target_gdframes[table] = df.copy(deep=True)

    def segment_addresses(self) -> None:
        """
        Converts address points into segmented attribution for NRN addrange and merges the resulting attributes to the
        source dataset representing NRN roadseg.
        """

        logger.info("Determining address segmentation requirement.")

        address_source = None
        roadseg_source = None
        segment_kwargs = None

        # Identify segmentation parameters and source datasets for roadseg and address points.
        for source, source_yaml in deepcopy(self.source_attributes).items():

            if "segment" in source_yaml["data"]:
                address_source = source
                segment_kwargs = source_yaml["data"]["segment"]

            if "conform" in source_yaml:
                if isinstance(source_yaml["conform"], dict):
                    if "roadseg" in source_yaml["conform"]:
                        roadseg_source = source

        # Trigger address segmentor.
        if all(val is not None for val in [address_source, roadseg_source, segment_kwargs]):

            logger.info(f"Address segmentation required. Beginning segmentation process.")

            # Copy data sources.
            addresses = self.source_gdframes[address_source].copy(deep=True)
            roadseg = self.source_gdframes[roadseg_source].copy(deep=True)

            # Execute segmentor.
            segmentor = Segmentor(source=self.source, addresses=addresses, roadseg=roadseg, **segment_kwargs)
            self.source_gdframes[roadseg_source] = segmentor()

            # Remove address source from attributes and dataframes references.
            # Note: segmented addresses will be joined to roadseg, therefore addrange and roadseg field mapping should
            # be defined within the same yaml.
            del self.source_attributes[address_source]
            del self.source_gdframes[address_source]

        else:
            logger.info("Address segmentation not required. Skipping segmentation process.")

    def split_strplaname(self) -> None:
        """
        Splits NRN strplaname records into multiple records if at least one nested column exists. The first and second
        records will contain the first and second nested values, respectively. NID linkages are repaired for the second
        instance of each split record since the linkage will have been broken.

        This process creates the left- and right-side representation which NRN strplaname is supposed to possess.
        """

        logger.info("Splitting strplaname to create left- and right-side representation.")

        # Compile nested column names.
        sample_value = self.target_gdframes["strplaname"].iloc[0]
        nested_flags = list(map(lambda val: isinstance(val, (np.ndarray, list)), sample_value))
        cols = sample_value.index[nested_flags].to_list()

        if len(cols):

            # Duplicate dataframe as left- and right-side representations.
            df_l = self.target_gdframes["strplaname"].copy(deep=True)
            df_r = self.target_gdframes["strplaname"].copy(deep=True)

            # Iterate nested columns and keep the 1st and 2nd values for left and right dataframes, respectively.
            for col in cols:
                df_l.loc[df_l.index, col] = df_l[col].map(itemgetter(0))
                df_r.loc[df_r.index, col] = df_r[col].map(itemgetter(1))

            # Generate new nids, uuids, and indexes for right dataframe, re-assign uuids as index for left dataframe.
            df_r["nid"] = [uuid.uuid4().hex for _ in range(len(df_r))]
            df_r["uuid"] = [uuid.uuid4().hex for _ in range(len(df_r))]
            df_r.index = df_r["uuid"]
            df_l.index = df_l["uuid"]

            # Update target dataframe.
            self.target_gdframes["strplaname"] = pd.concat([df_l, df_r], ignore_index=False).copy(deep=True)

            # Generate lookup dict between old and new nids for right dataframe.
            nid_lookup = dict(zip(df_l["nid"], df_r["nid"]))

            # Repair nid linkages.
            logger.info("Repairing strplaname.nid linkages.")

            # Define nid linkages.
            linkages = {
                "addrange": ["r_offnanid"]
            }

            # Iterate nid linkages.
            for table in set(linkages).intersection(set(self.target_gdframes)):
                for field in linkages[table]:

                    # Repair nid linkage.
                    series = self.target_gdframes[table][field].copy(deep=True)
                    self.target_gdframes[table].loc[series.index, field] = series.map(
                        lambda val: itemgetter(val)(nid_lookup))

                    # Quantify and log modifications.
                    mods_count = (series != self.target_gdframes[table][field]).sum()
                    if mods_count:
                        logger.warning(f"Repaired {mods_count} linkage(s) between strplaname.nid - {table}.{field}.")

            # Update altnamlink.
            if "altnamlink" in self.target_gdframes:

                logger.info("Updating altnamlink.")

                # Duplicate records.
                df_first = self.target_gdframes["altnamlink"].copy(deep=True)
                df_second = self.target_gdframes["altnamlink"].copy(deep=True)

                # Generate new strnamenids, uuids, and indexes for second dataframe.
                df_second["strnamenid"] = [uuid.uuid4().hex for _ in range(len(df_second))]
                df_second["uuid"] = [uuid.uuid4().hex for _ in range(len(df_second))]
                df_second.index = df_second["uuid"]

                # Update columns, if required.
                df_second["credate"] = datetime.today().strftime("%Y%m%d")
                df_second["revdate"] = self.defaults["altnamlink"]["revdate"]
                df_second["strnamenid"] = df_second["strnamenid"].map(lambda val: itemgetter(nid_lookup)(val))

                # Store results.
                self.target_gdframes["altnamlink"] = pd.concat([df_first, df_second],
                                                               ignore_index=False).copy(deep=True)

    def execute(self) -> None:
        """Executes an NRN stage."""

        self.download_previous_vintage()
        self.compile_source_attributes()
        self.compile_target_attributes()
        self.gen_source_dataframes()
        self.segment_addresses()
        self.gen_target_dataframes()
        self.apply_field_mapping()
        self.split_strplaname()
        self.recover_missing_datasets()
        self.apply_domains()
        self.clean_datasets()
        self.filter_and_relink_strplaname()
        helpers.export(self.target_gdframes, self.output_path)


@click.command()
@click.argument("source", type=click.Choice("ab bc mb nb nl ns nt nu on pe qc sk yt".split(), False))
@click.option("--remove / --no-remove", "-r", default=False, show_default=True,
              help="Remove pre-existing files within the data/interim directory for the specified source.")
@click.option("--exclude-old / --no-exclude-old", "-e", default=False, show_default=True,
              help="Excludes the previous NRN vintage for the specified source from being removed if remove=True. "
                   "Option has no effect if remove=False.")
def main(source: str, remove: bool = False, exclude_old: bool = False) -> None:
    """
    Executes an NRN stage.

    :param str source: abbreviation for the source province / territory.
    :param bool remove: removes pre-existing files within the data/interim directory for the specified source, default
        False.
    :param bool exclude_old: excludes the previous NRN vintage for the specified source from being removed if
        remove=True, default False. Option has no effect if remove=False.
    """

    try:

        with helpers.Timer():
            stage = Stage(source, remove, exclude_old)
            stage.execute()

    except KeyboardInterrupt:
        logger.exception("KeyboardInterrupt: Exiting program.")
        sys.exit(1)


if __name__ == "__main__":
    main()
