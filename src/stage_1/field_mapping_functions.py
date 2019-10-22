import logging
import re


logger = logging.getLogger()


def regex_find(val, pattern, match_index, group_index):
    """Extracts a value's nth match (index) from the nth match group (index) based on a regular expression pattern."""

def regex_sub(val, pattern_from, pattern_to):
    """Substitutes one regular expression pattern with another."""

def split_record(val, fields):
    """Splits records into multiple records whenever the given fields are not equal."""

def strip_attributes(val, attributes):
    """Sequentially strips values of one or more fields from the target field."""
