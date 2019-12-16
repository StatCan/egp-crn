import calendar
import logging
from datetime import datetime


logger = logging.getLogger()


def strip_whitespace(val):
    """Strips leading and trailing whitespace from the given value."""

    return val.strip()


def validate_dates(credate, revdate, default):
    """
    Applies a set of validations to credate and revdate fields.
    Parameter default is assumed to be identical for credate and revdate fields.
    """

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


def validate_nbrlanes(nbrlanes, default):
    """Applies a set of validations to nbrlanes field."""

    # Validation: ensure 1 <= nbrlanes <= 8.
    if str(nbrlanes) != str(default):
        if not 1 <= int(nbrlanes) <= 8:
            raise ValueError("Invalid value for nbrlanes = \"{}\". Value must be between 1 and 8.".format(nbrlanes))

    return nbrlanes


def validate_pavement(pavstatus, pavsurf, unpavsurf):
    """Applies a set of validations to pavstatus, pavsurf, and unpavsurf fields."""

    if int(pavstatus) == 1:
        if int(pavsurf) == 0 or int(unpavsurf) != 0:
            raise ValueError(
                "Invalid combination for pavstatus = \"{}\", pavsurf = \"{}\", unpavsurf = \"{}\". When pavstatus is 1,"
                " pavsurf must not be 0 and unpavsurf must be 0.".format(pavstatus, pavsurf, unpavsurf))

    if int(pavstatus) == 2:
        if int(pavsurf) != 0 or int(unpavsurf) == 0:
            raise ValueError(
                "Invalid combination for pavstatus = \"{}\", pavsurf = \"{}\", unpavsurf = \"{}\". When pavstatus is 2,"
                " pavsurf must be 0 and unpavsurf must not be 0.".format(pavstatus, pavsurf, unpavsurf))

    return pavstatus, pavsurf, unpavsurf


def validate_roadclass_rtnumber1(roadclass, rtnumber1, default):
    """
    Applies a set of validations to roadclass and rtnumber1 fields.
    Parameter default should refer to field rtnumber1.
    """

    # Validation: ensure rtnumber1 is populated when roadclass == 1 or 2.
    if int(roadclass) in (1, 2):
        if str(rtnumber1) == str(default):
            raise ValueError("Invalid value for rtnumber1 = \"{}\". When roadclass is 1 or 2, rtnumber1 must not be "
                             "the default field value = \"{}\".".format(rtnumber1, default))

    return roadclass, rtnumber1


def validate_speed(speed, default):
    """Applies a set of validations to speed field."""

    if str(speed) != str(default):

        # Validation: ensure 5 <= speed <= 120.
        if not 5 <= int(speed) <= 120:
            raise ValueError("Invalid value for speed = \"{}\". Value must be between 5 and 120.".format(speed))

        # Validation: ensure speed is a multiple of 5.
        if int(speed) % 5 != 0:
            raise ValueError("Invalid value for speed = \"{}\". Value must be a multiple of 5.".format(speed))

    return speed
