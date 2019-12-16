import calendar
import logging
import sys
from datetime import datetime


logger = logging.getLogger()


def strip_whitespace(val):
    """Strips leading and trailing whitespace from the given value."""

    return val.strip()


def validate_dates(credate, revdate, default):
    """Applies a set of validations to credate and revdate fields."""

    try:

        credate, revdate, default = map(str, [credate, revdate, default])

        # Get current date.
        today = datetime.today().strftime("%Y%m%d")

        # Validation.
        def validate(date):

            if date != default:

                # Validation: length must be 4, 6, or 8.
                if len(date) not in (4, 6, 8):
                    raise ValueError("Invalid length for credate / revdate = \"{}\".".format(date))

                # Rectification: default to 01 for missing month and day values.
                while len(date) in (4, 6):
                    date += "01"

                # Validation: valid values for day, month, year (1960+).
                year, month, day = map(int, [date[:4], date[4:6], date[6:8]])

                # Year.
                if not 1960 <= year <= int(today[:4]):
                    raise ValueError("Invalid year for credate / revdate at index 0:3 = \"{}\".".format(year))

                # Month.
                if month not in range(1, 12 + 1):
                    raise ValueError("Invalid month for credate / revdate at index 4:5 = \"{}\".".format(month))

                # Day.
                if not 1 <= day <= calendar.mdays[month]:
                    if not all([day == 29, month == 2, calendar.isleap(year)]):
                        raise ValueError("Invalid day for credate / revdate at index 6:7 = \"{}\".".format(day))

                # Validation: ensure value <= today.
                if year == today[:4]:
                    if not all([month <= today[4:6], day <= today[6:8]]):
                        raise ValueError("Invalid date for credate / revdate = \"{}\". "
                                         "Date cannot be in the future.".format(date, today))

            return date

        # Validation: individual date validations.
        credate = validate(credate)
        revdate = validate(revdate)

        # Validation: ensure credate <= revdate.
        if credate != default and revdate != default:
            if not int(credate) <= int(revdate):
                raise ValueError("Invalid date combination for credate = \"{}\", revdate = \"{}\". "
                                 "credate must precede or equal revdate.".format(credate, revdate))

        return credate, revdate

    except ValueError:
        raise
