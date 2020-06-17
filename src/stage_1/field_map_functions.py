import logging
import numpy as np
import os
import pandas as pd
import re
import sys
import uuid
from operator import attrgetter, itemgetter

sys.path.insert(1, os.path.join(sys.path[0], ".."))
import helpers


logger = logging.getLogger()
domains = helpers.compile_domains(mapped_lang="en")


def apply_domain(**kwargs):
    """Calls helpers.apply_domains to allow it's usage as a field mapping function."""

    return helpers.apply_domain(**kwargs)


def direct(series, cast_type=None):
    """
    Returns the given series. Intended to provide a function call for direct (1:1) field mapping.
    Parameter 'cast_type' expected to be a string representation of a python data type. Example: "str", "int", etc.

    Possible yaml construction of direct field mapping:

    1) target_field:                                2) target_field: source_field or raw value
         fields: source_field or raw value
         functions:
           - function: direct
             cast_type: 'int'
    """

    try:

        # Return uncasted series.
        if cast_type is None:
            return series

        # Return casted series.
        elif cast_type == "float":
            return series.astype(float)
        elif cast_type == "int":
            return series.astype("Int32" if series.dtype.name[-2:] == "32" else "Int64")
        elif cast_type == "str":
            return series.astype(str).replace("nan", np.nan)
        else:
            logger.exception("Invalid cast type \"{}\". Must be one of float, int, str.".format(cast_type))
            sys.exit(1)

    except (TypeError, ValueError):
        logger.exception("Unable to cast series from {} to {}.".format(series.dtype.name, cast_type))
        sys.exit(1)


def gen_uuid(series):
    """Returns a uuid4 hexadecimal string for each record in the series."""

    return pd.Series([uuid.uuid4().hex for _ in range(len(series))], index=series.index)


def incrementor(series, start=1, step=1):
    """Returns an integer sequence series using the given start and step."""

    if not all(isinstance(param, int) for param in (start, step)):
        logger.exception(f"Unable to generate sequence. One or more input variables is not an integer.")

    stop = (len(series) * step) + start
    return pd.Series(range(start, stop, step), index=series.index)


def regex_find(series, pattern, match_index, group_index, strip_result=False, sub_inplace=None):
    """
    For each value in a series, extracts the nth match (index) from the nth match group (index) based on a regular
    expression pattern.
    Parameter 'group_index' can be an int or list of ints, the returned value will be at the first index with a match.
    Parameter 'strip_result' returns the entire value except for the extracted substring.
    Parameter 'sub_inplace' takes the same parameters as re.sub. This allows regex to match against a modified string
    yet preserve the unmodified string. For example, to match 'de la' from the string 'Chemin-de-la-Grande-Rivière',
    sub_inplace can call re.sub to replace '-' with ' ', then substitute the match's indexes from the original string
    to preserve hyphens in the remainder of the string.
    """

    def regex_find_multiple_idx(val, pattern):
        """
        Return resulting regex string based on multiple group indexes. Returns the result from the first group index
        with a match.
        """

        try:

            matches = re.finditer(pattern, re.sub(**sub_inplace, string=val) if sub_inplace else val, flags=re.I)
            result = [[itemgetter(*group_index)(m.groups()), m.start(), m.end()] for m in matches][match_index]
            result[0] = [grp for grp in result[0] if grp != "" and not pd.isna(grp)][0]

            # Return stripped result, if required.
            return strip(val, result) if strip_result else itemgetter(0)(result)

        except (IndexError, ValueError):
            return val if strip_result else np.nan

    def regex_find_single_idx(val, pattern):
        """Return resulting regex string based on a single group index."""

        try:

            matches = re.finditer(pattern, re.sub(**sub_inplace, string=val) if sub_inplace else val, flags=re.I)
            result = [[m.groups()[group_index], m.start(), m.end()] for m in matches][match_index]

            # Return stripped result, if required.
            return strip(val, result) if strip_result else itemgetter(0)(result)

        except (IndexError, ValueError):
            return val if strip_result else np.nan

    def strip(val, result):
        """Strip result from original value."""

        try:

            start, end = result[1:]

            # Reset start index to avoid stacking spaces and hyphens.
            if start > 0 and end < len(val):
                while val[start - 1] == val[end] and val[end] in {" ", "-"}:
                    start -= 1

            return "".join(map(str, [val[:start], val[end:]]))

        except (IndexError, ValueError):
            return result if strip_result else np.nan

    # Validate inputs.
    pattern = validate_regex(pattern)
    validate_dtypes("match_index", match_index, [int, np.int_])
    validate_dtypes("group_index", group_index, [int, np.int_, list])
    if isinstance(group_index, list):
        for index, i in enumerate(group_index):
            validate_dtypes("group_index[{}]".format(index), i, [int, np.int_])
    validate_dtypes('strip_result', strip_result, [bool, np.bool_])
    if sub_inplace:
        validate_dtypes("sub_inplace", sub_inplace, dict)
        if {"pattern", "repl"}.issubset(set(sub_inplace.keys())):
            sub_inplace["pattern"] = validate_regex(sub_inplace["pattern"])
            sub_inplace["repl"] = validate_regex(sub_inplace["repl"])
            sub_inplace["flags"] = re.I
        else:
            logger.exception("Invalid input for sub_inplace. Missing one or more required re.sub kwargs: pattern, "
                             "repl.")
            sys.exit(1)

    # Replace empty or nan values with numpy nan.
    series.loc[(series == "") | (series.isna())] = np.nan

    # Compile valid records.
    series_valid = series[~series.isna()].copy(deep=True)

    # Compile regex results, based on required group indexes.
    if isinstance(group_index, int) or isinstance(group_index, np.int_):
        results = series_valid.map(lambda val: regex_find_single_idx(val, pattern))
    else:
        results = series_valid.map(lambda val: regex_find_multiple_idx(val, pattern))

    # Strip leading and trailing whitespaces and hyphens.
    results = results.map(lambda val: str(val).strip(" -"))

    # Update series with results.
    series.loc[series_valid.index] = results

    return series


def regex_sub(series, **kwargs):
    """Applies value substitution via re.sub."""

    # Validate inputs.
    validate_dtypes("kwargs", kwargs, dict)
    if {"pattern", "repl"}.issubset(set(kwargs.keys())):
        kwargs["pattern"] = validate_regex(kwargs["pattern"])
        kwargs["repl"] = validate_regex(kwargs["repl"])
        kwargs["flags"] = re.I
    else:
        logger.exception("Invalid input. Missing one or more required re.sub kwargs: pattern, repl.")

    # Replace empty or nan values with numpy nan.
    series.loc[(series == "") | (series.isna())] = np.nan

    # Compile valid records.
    series_valid = series[~series.isna()].copy(deep=True)

    # Apply regex substitution.
    series.loc[series_valid.index] = series_valid.map(lambda val: re.sub(**kwargs, string=val))

    return series


def validate_dtypes(name, val, dtypes):
    """Checks if the given value is from the given dtype(s)."""

    if not isinstance(dtypes, list):
        dtypes = [dtypes]

    if any([isinstance(val, dtype) for dtype in dtypes]):
        return True

    else:
        logger.exception(f"Invalid data type for {name}: {val}. Expected one of "
                         f"{list(map(attrgetter('__name__'), dtypes))} but received {type(val).__name__}.")
        sys.exit(1)


def validate_regex(pattern):
    """
    Validates a regular expression.
    Replaces any keywords of format: 'domain_{table}_{field}' that are within curly braces () with the '|' joined
    domain values of the parsed table and field names.
    """

    try:

        # Compile regular expression.
        re.compile(pattern)

        # Substitute domain keywords with values.
        # Process: iterate keyword matches, parse the resulting keywords and use as domain lookup keys, replace the
        # original values with '|' joined domain values.
        for kw in set([itemgetter(0)(match.groups()) for match in
                       re.finditer(r"\(domain_(.*?)\)", pattern, flags=re.I)]):

            table, field = kw.split("_")
            domain = domains[table][field]["values"]

            if domain:
                pattern = pattern.replace(kw, "|".join(map(str, domain)))

        return pattern

    except re.error:
        logger.exception(f"Invalid regular expression: {pattern}.")
        sys.exit(1)
