import logging
import pandas
import re
import sys
from operator import itemgetter


logger = logging.getLogger()


def regex_find(val, pattern, match_index, group_index, strip_attributes_flag=False):
    """
    Extracts a value's nth match (index) from the nth match group (index) based on a regular expression pattern.
    Case ignored by default.
    Parameter 'group_index' can be an int or list of ints, the returned value will be at the first index with a match.
    """

    # Validate inputs.
    validate_regex(pattern)
    validate_dtype("match_index", match_index, int)
    if isinstance(group_index, list):
        for index, i in enumerate(group_index):
            validate_dtype("group_index[{}]".format(index), i, int)
    else:
        validate_dtype("group_index", group_index, int)
    validate_dtype('strip_attributes_flag', strip_attributes_flag, bool)

    # Apply and return regex value, or pandas' NaN.
    try:

        # Single group index.
        if isinstance(group_index, int):
            result = [[match.groups()[group_index], match.start(), match.end()] for match in re.finditer(pattern, val, flags=re.IGNORECASE)][match_index]

        # Multiple group indexes.
        else:
            result = [[itemgetter(*group_index)(match.groups()), match.start(), match.end()] for match in re.finditer(pattern, val, flags=re.IGNORECASE)][match_index]
            result[0] = [grp for grp in result[0] if grp not in (None, "")][0]

        return result[0] if strip_attributes_flag else result[1:]

    except IndexError:
        return pandas.np.nan


def regex_sub(val, pattern_from, pattern_to):
    """
    Substitutes one regular expression pattern with another.
    Case ignored by default.
    """

    # Validate inputs.
    validate_regex(pattern_from)
    validate_regex(pattern_to)

    # Apply and return regex value.
    return re.sub(pattern_from, pattern_to, val, flags=re.IGNORECASE)


def split_record(val, fields):
    """Splits records into multiple records whenever the given fields are not equal."""

    # Validate inputs.
    validate_dtype("fields", fields, list)
    for index, field in enumerate(fields):
        validate_dtype("fields[{}]".format(index), field, str)

    # . . . .


def strip_attribute(val, attributes):
    """Sequentially strips values of one or more fields from the target field."""

    # Validate inputs.
    validate_dtype("attributes", attributes, list)
    for index, attribute in enumerate(attributes):
        validate_dtype("attributes[{}]".format(index), attribute, str)

    # . . . .


def validate_dtype(val_name, val, dtype):
    """Validates a data type."""

    if isinstance(val, dtype):
        return True
    else:
        logger.error("Validation failed. Invalid data type for \"{}\": \"{}\". Expected {} but received {}.".format(
            val_name, val, dtype.__name__, type(val).__name__))
        sys.exit(1)


def validate_regex(pattern):
    """Validates a regular expression."""

    try:
        re.compile(pattern)
        return True
    except re.error:
        logger.error("Validation failed. Invalid regular expression: \"{}\".".format(pattern))
        sys.exit(1)
