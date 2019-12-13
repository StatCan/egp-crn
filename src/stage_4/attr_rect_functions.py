import calendar
import logging
import sys
from datetime import datetime


logger = logging.getLogger()


def strip_whitespace(val):
    """Strips leading and trailing whitespace from the given value."""

    return val.strip()


def validate_dates(credate, revdate, default):
    """Applies a set of validations to CREDATE and REVDATE fields."""

    try:

        credate, revdate, default = map(str, [credate, revdate, default])

        # Get current date.
        today = datetime.today().strftime("%Y%m%d")

        # Apply validations.
        for date in [d for d in (credate, revdate) if d != default]:

            # Validation: length must be 4, 6, or 8.
            if len(date) not in (4, 6, 8):
                raise ValueError("Invalid length for CREDATE / REVDATE = \"{}\".".format(date))

            # Rectification: default to 01 for missing month and day values.
            while len(date) in (4, 6):
                date += "01"

            # Validation: valid values for day, month, year (1960+).
            day, month, year = map(int, [date[:4], date[4:6], date[6:8]])

            # Year.
            if not 1960 <= year <= int(today[:4]):
                raise ValueError("Invalid year for CREDATE / REVDATE at index 0:3 = \"{}\".".format(year))

            # Month.
            if month not in range(1, 12 + 1):
                raise ValueError("Invalid month for CREDATE / REVDATE at index 4:5 = \"{}\".".format(month))

            # Day.
            if not 1 <= day <= calendar.mdays[month]:
                if not all([day == 29, month == 2, calendar.isleap(year)]):
                    raise ValueError("Invalid day for CREDATE / REVDATE at index 6:7 = \"{}\".".format(day))

            # Validation: ensure value <= today.
            if year == today[:4]:
                if not all([month <= today[4:6], day <= today[6:8]]):
                    raise ValueError("Invalid date for CREDATE / REVDATE = \"{}\". "
                                     "Date cannot be in the future.".format(date, today))

        # Validation: ensure CREDATE <= REVDATE.
        if credate != default and revdate != default:
            if not int(credate) <= int(revdate):
                raise ValueError("Invalid date combination for CREDATE = \"{}\", REVDATE = \"{}\". "
                                 "CREDATE must precede or equal REVDATE.".format(credate, revdate))

        return credate, revdate

    except ValueError as e:
        logger.exception("ValueError: {}".format(e))
        sys.exit(1)
