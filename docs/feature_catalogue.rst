**********************************
Feature Catalogue - Segmented View
**********************************

.. include :: <isonum.txt>

.. note::
    The description of features and attributes provided in this catalogue is largely based on the standard *ISO
    14825 — Intelligent transport systems — Geographic Data Files (GDF) — Overall data specification* resulting from
    technical committee ISO / TC 204.

    This catalogue was adapted from the international standard *ISO 19110 — Geographic information — Methodology for
    feature cataloguing* prepared by technical committee ISO/TC 211.

.. contents::
   :depth: 4

Acronyms and Abbreviations
==========================

.. glossary::
    CMAS
        Circular Map Accuracy Standard

    DEM
        Digital Elevation Model

    GPS
        Global Positioning System

    ID
        Identifier

    ISO/TC
        International Organisation for Standardisation, Technical Committee

    NatProvTer
        National, Provincial, or Territorial

    NID
        National Identifier

    NRCan
        Natural Resources Canada

    NRN
        National Road Network

    UUID
        Universal Unique Identifier

Terms and Definitions
=====================

Attribute
    Characteristic of a feature. For example, number of lanes or pavement status.

Class
    Description of a set of objects that share the same attributes, operations, methods, relationships, and semantics.
    A class does not always have an associated geometry (e.g., address range class).

Feature
    Digital representation of a real world phenomenon.

Ferry Connection
    The average route a ferryboat takes when transporting vehicles between two fixed locations on the Road Network.
    Two Junctions always bound a Ferry Connection.

Network Linear Element
    Abstract class of a Road Element and Ferry Connection.

Object
    An object is an instance of a class.

Road Element
    A road is a linear section of the earth designed for or the result of vehicular movement. A Road Element is the
    representation of a road between Junctions. A Road Element is always bounded by two Junctions. A Road Element is
    composed of one or more than one contiguous Road Segments.

Segment
    Portion of a Network Linear Element that has a common set of defined characteristics (attributes).

Universal Unique Identifier (UUID)
    The definition and method used for the generation of a Universal Unique Identifier is defined in the document
    National Vector Data – Identification Rules available on the GeoBase portal (www.geobase.ca), under the National
    Road Network Data section.

Object Metadata
===============

The attributes described in the section object metadata apply to all feature classes (except for Alternate
Name Link).

Acquisition Technique
---------------------

The type of data source or technique used to populate (create or revise) the dataset.

:Domain:

====  =========================  ==========
Code  Label                      Definition
====  =========================  ==========
-1    Unknown                    Impossible to determine.
0     None                       No value applies.
1     Other                      All possible values not explicitly mentioned in the domain.
2     GPS                        Data collected using a GPS device.
3     Orthoimage                 Satellite imagery orthorectified.
4     Orthophoto                 Aerial photo orthorectified.
5     Vector Data                Vector digital data.
6     Paper Map                  Conventional sources of information like maps or plans.
7     Field Completion           Information gathered from people directly on the field.
8     Raster Data                Data resulting from a scanning process.
9     Digital Elevation Model    Data coming from a Digital Elevation Model (DEM).
10    Aerial Photo               Aerial photography not orthorectified.
11    Raw Imagery Data           Satellite imagery not orthorectified.
12    Computed                   Geometric information that has been computed (not captured).
====  =========================  ==========

Coverage
--------

This value indicates if this set of metadata covers the full length of the Network Linear Element or only a
portion of it.

:Domain:

====  ===========  ==========
Code  Label        Definition
====  ===========  ==========
-1    Unknown      Impossible to determine.
1     Complete     Metadata applies on the entire geometry or attribute event.
2     Partial      Metadata applies on a portion of the geometry or attribute event.
====  ===========  ==========


Creation Date
-------------

The date of data creation.

:Domain: A date in the format YYYYMMDD or "Unknown". If the month or the day is unknown, corresponding characters are
    left blank.

    Examples: 20060630, 200606, 2006.
:Data Type: Character (8)

Dataset Name
------------

Province or Territory covered by the dataset.

:Domain:

====  =====
Code  Label
====  =====
1     Newfoundland and Labrador
2     Nova Scotia
3     Prince Edward Island
4     New Brunswick
5     Quebec
6     Ontario
7     Manitoba
8     Saskatchewan
9     Alberta
10    British Columbia
11    Yukon Territory
12    Northwest Territories
13    Nunavut
====  =====

Planimetric Accuracy
--------------------

The planimetric accuracy expressed in meters as the circular map accuracy standard (CMAS).

:Domain: [-1,1..n]
:Data Type: Integer
    « -1 » when the value is unknown

Provider
--------

The affiliation of the organization that generated (created or revised) the object.

:Domain:

====  =========================  ==========
Code  Label                      Definition
====  =========================  ==========
1     Other                      Other value.
2     Federal                    Federal departments or agencies.
3     Provincial / Territorial   Provincial / territorial departments or agencies.
4     Municipal                  Municipal departments or agencies.
====  =========================  ==========

Revision Date
-------------

The date of data revision.

:Domain: A date in the format YYYYMMDD or "Unknown". If the month or the day is unknown, corresponding characters
    are left blank.

    Examples: 20060630, 200606, 2006.
:Data Type: Character (8)

Standard Version
----------------

The version number of the GeoBase Product specifications.

:Domain: [2.0]
:Data Type: Character (10)

Address Range
=============

A set of attributes representing the address of the first and last building located along a side of the entire Road
Element or a portion of it.

:Is Abstract: No
:Geometry:

Attribute Section
-----------------

Alternate Street Name NID (left, right)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The identifier used to link an address range to its alternate street name. A specific value is defined for the left
and right sides of the Road Element.

:Domain: A UUID or "None" when no value applies. Example: 69822b23d217494896014e57a2edb8ac
:Data Type: Character (32)

Digitizing Direction Flag (left, right)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Indicates if the attribute event follows the same direction as the digitizing of the Road Element. A specific value
is defined for the left and right sides of the Road Element.

:Domain:

====  =========================  ==========
Code  Label                      Definition
====  =========================  ==========
1     Same Direction             Attribute event and Road Element geometry are in the same direction.
2     Opposite Direction         Attribute event and Road Element geometry are in opposite directions.
3     Not Applicable             Indication of the digitizing direction of the Road Element not needed for the attribute event.
====  =========================  ==========

First House Number (left, right)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The first house number address value along a particular side (left or right) of a Road Element. A specific value is
defined for the left and right sides of the Road Element.

:Domain: [-1..n] The value "0" is used when no value applies. The value "-1" is used when the value is unknown.
:Data Type: Integer

First House Number Suffix (left, right)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A non-integer value, such as a fraction (e.g. 1⁄4) or a character (e.g. A) that sometimes follows the house number
address value. A specific value is defined for the left and right sides of the Road Element.

:Domain: A non-integer value or "None" when no value applies.
:Data Type: Character (10)

First House Number Type (left, right)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Method used to populate the address range. A specific value is defined for the left and right sides of the Road Element.

:Domain:

====  =========================  ==========
Code  Label                      Definition
====  =========================  ==========
-1    Unknown                    Due to the source, the house number type is not known.
0     None                       Absence of a house along the Road Element.
1     Actual Located             Qualifier indicating that the house number is located at its "real world" position along a Road Element.
2     Actual Unlocated           Qualifier indicating that the house number is located at one end of the Road Element. This may be or may not be its "real world" position.
3     Projected                  Qualifier indicating that the house number is planned, figured or estimated for the future and is located (at one end) at the beginning or the end of the Road Element.
4     Interpolated               Qualifier indicating that the house number is calculated from two known house numbers which are located on either side. By convention, the house is positioned at one end of the Road Element.
====  =========================  ==========

House Number Structure (left, right)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The type of house numbering (or address numbering) method applied to one side of a particular Road Element. A specific
value is defined for the left and right sides of the Road Element.

:Domain:

====  =========================  ==========
Code  Label                      Definition
====  =========================  ==========
-1    Unknown                    Impossible to determine.
0     None                       No house numbers at all. There are no houses (or addressed dwellings) along a particular side of a Road Element.
1     Even                       The house numbers appear as even numbers in a sequentially sorted order (ascending or descending) when moving from one end of the Road Element to the other. Numeric completeness of the series is not a requirement. An even house number series that has missing numbers but is sequentially sorted is considered Even. An example is the series (2, 4, 8, 18, 22).
2     Odd                        The house numbers appear as odd numbers in a sequentially sorted order (ascending or descending) when moving from one end of the Road Element to the other. Numeric completeness of the series is not a requirement. An odd house number series that has missing numbers but is sequentially sorted is considered Odd. Examples are the series (5, 7, 9, 11, 13) and (35, 39, 43, 69, 71, 73, 85).
3     Mixed                      The house numbers are odd and even on the same side of a Road Element in a sequentially sorted order (ascending or descending) when moving from one end of the Road Element to the other. Numeric completeness of the series is not a requirement. An odd and even house number series that has missing numbers but is sequentially sorted is considered Mixed. Examples are the series (5, 6, 7, 9, 10, 13) and (24, 27, 30, 33, 34, 36).
4     Irregular                  Means the house numbers do not occur in any sorted order.
====  =========================  ==========

Last House Number (left, right)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The last house number address value along a particular side (left or right) of a Road Element. A specific value is
defined for the left and right sides of the Road Element.

:Domain: [-1..n] The value "0" is used when no value applies. The value "-1" is used when the value is unknown.
:Data Type: Integer

Last House Number Suffix (left, right)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A non-integer value, such as a fraction (e.g. 1⁄4) or a character (e.g. A) that sometimes follows the house number
address value. A specific value is defined for the left and right sides of the Road Element.

:Domain: A non-integer value or "None" when no value applies.
:Data Type: Character (10)

Last House Number Type (left, right)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Method used to populate the address range. A specific value is defined for the left and right sides of the Road Element.

:Domain:

====  =========================  ==========
Code  Label                      Definition
====  =========================  ==========
-1    Unknown                    Due to the source, the house number type is not known.
0     None                       Absence of a house along the Road Element.
1     Actual Located             Qualifier indicating that the house number is located at its "real world" position along a Road Element.
2     Actual Unlocated           Qualifier indicating that the house number is located at one end of the Road Element. This may be or may not be its "real world" position.
3     Projected                  Qualifier indicating that the house number is planned, figured or estimated for the future and is located (at one end) at the beginning or the end of the Road Element.
4     Interpolated               Qualifier indicating that the house number is calculated from two known house numbers which are located on either side. By convention, the house is positioned at one end of the Road Element.
====  =========================  ==========

NID
^^^

A national unique identifier.

:Domain: A UUID.

    Example: 69822b23d217494896014e57a2edb8ac
:Data Type: Character (32)

Official Street Name NID (left, right)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The identifier used to link an address range to its recognized official street name. A specific value is defined for
the left and right sides of the Road Element.

:Domain: A UUID or "None" when no value applies.

    Example: 69822b23d217494896014e57a2edb8ac
:Data Type: Character (32)

Reference System Indicator (left, right)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

An indication of whether the physical address of all or a portion of a Road Element is based on a particular
addressing system. A specific value is defined for the left and right sides of the Road Element.

:Domain:

====  =========================  ==========
Code  Label                      Definition
====  =========================  ==========
-1    Unknown                    Impossible to determine.
0     None                       No reference system indicator.
1     Civic
2     Lot and Concession
3     911 Measured
4     911 Civic
5     DLS Townships              Dominion Land Survey, survey method dominant in the Prairie provinces.
====  =========================  ==========

Object Metadata
^^^^^^^^^^^^^^^

Refer to the attributes describe in the section object metadata.

Alternate Name Link
===================

A linkup table establishing one or many relations between address ranges and their non-official street and place names
used or known by the general public.

:Is Abstract: No
:Geometry:

Attribute Section
-----------------

NID
^^^

A national unique identifier.

:Domain: A UUID.

    Example: 69822b23d217494896014e57a2edb8ac
:Data Type: Character (32)

Street Name NID
^^^^^^^^^^^^^^^

The NID of the non official street and place name.

:Domain: A UUID.

    Example: 69822b23d217494896014e57a2edb8ac
:Data Type: Character (32)

Creation Date
^^^^^^^^^^^^^

The date of data creation.

:Domain: A date in the format YYYYMMDD or "Unknown". If the month or the day is unknown, corresponding characters are
    left blank.

    Examples: 20060630, 200606, 2006.
:Data Type: Character (8)

Dataset Name
^^^^^^^^^^^^

Province or Territory covered by the dataset.

:Domain:

====  =====
Code  Label
====  =====
1     Newfoundland and Labrador
2     Nova Scotia
3     Prince Edward Island
4     New Brunswick
5     Quebec
6     Ontario
7     Manitoba
8     Saskatchewan
9     Alberta
10    British Columbia
11    Yukon Territory
12    Northwest Territories
13    Nunavut
====  =====

Standard Version
^^^^^^^^^^^^^^^^

The version number of the GeoBase Product specifications.

:Domain: [2.0]
:Data Type: Character (10)

Blocked Passage
===============

Indication of a physical barrier on a Road Element built to prevent or control further access.

:Is Abstract: No
:Geometry: Point

Attribute Section
-----------------

Blocked Passage Type
^^^^^^^^^^^^^^^^^^^^

The type of blocked passage as an indication of the fact whether it is removable.

:Domain:

====  =========================  ==========
Code  Label                      Definition
====  =========================  ==========
-1    Unknown                    A blocked passage for which the specific type is unknown.
1     Permanently Fixed          The barrier cannot be removed without destroying it. Heavy equipment needed in order to allow further access. Examples of permanently fixed blocked passage are concrete blocks or a mound of earth.
2     Removable                  The barrier is designed to free the entrance to the (other side of the) Road Element that it is blocking. Further access easily allowed when so desired.
====  =========================  ==========

NID
^^^

A national unique identifier.

:Domain: A UUID.

    Example: 69822b23d217494896014e57a2edb8ac
:Data Type: Character (32)

Road Element NID
^^^^^^^^^^^^^^^^

The NID of the Road Element on which the point geometry is located.

:Domain: A UUID.

    Example: 69822b23d217494896014e57a2edb8ac
:Data Type: Character (32)

Object Metadata
^^^^^^^^^^^^^^^

Refer to the attributes describe in the section object metadata.

Ferry Connection Segment
========================

The average route a ferryboat takes when transporting vehicles between two fixed locations on the road network.

:Is Abstract: No
:Geometry: Line

Attribute Section
-----------------

Closing Period
^^^^^^^^^^^^^^

The period in which the road or ferry connection is not available to the public.

:Domain:

====  =========================  ==========
Code  Label                      Definition
====  =========================  ==========
-1    Unknown                    It is not possible to determine if there is a closing period.
0     None                       There is no closing period. The road or ferry connection is open year round.
1     Summer                     Period of the year for which the absence of ice and snow prevent the access to the road or ferry connection.
2     Winter                     Period of the year for which ice and snow prevent the access to the road or ferry connection.
====  =========================  ==========

Ferry Segment ID
^^^^^^^^^^^^^^^^

A unique identifier within a dataset assigned to each Ferry Connection Segment.

:Domain: [1..n]
:Data Type: Integer

Functional Road Class
^^^^^^^^^^^^^^^^^^^^^

A classification based on the importance of the role that the Road Element or Ferry Connection performs in the
connectivity of the total road network.
:Domain:

====  =========================  ==========
Code  Label                      Definition
====  =========================  ==========
1     Freeway                    An unimpeded, high-speed controlled access thoroughfare for through traffic with typically no at- grade intersections, usually with no property access or direct access, and which is accessed by a ramp. Pedestrians are prohibited.
2     Expressway / Highway       A high-speed thoroughfare with a combination of controlled access intersections at any grade.
3     Arterial                   A major thoroughfare with medium to large traffic capacity.
4     Collector                  A minor thoroughfare mainly used to access properties and to feed traffic with right of way.
5     Local / Street             A low-speed thoroughfare dedicated to provide full access to the front of properties.
6     Local / Strata             A low-speed thoroughfare dedicated to provide access to properties with potential public restriction such as: trailer parks, First Nations, strata, private estates, seasonal residences.
7     Local / Unknown            A low-speed thoroughfare dedicated to provide access to the front of properties but for which the access regulations are unknown.
8     Alleyway / Lane            A low-speed thoroughfare dedicated to provide access to the rear of properties.
9     Ramp                       A system of interconnecting roadways providing for the controlled movement between two or more roadways.
10    Resource / Recreation      A narrow passage whose primary function is to provide access for resource extraction and may also have serve in providing public access to the backcountry.
11    Rapid Transit              A thoroughfare restricted to public transit buses.
12    Service Lane               A stretch of road permitting vehicles to come to a stop along a freeway or highway. Scale, service lane, emergency lane, lookout, and rest area.
13    Winter                     A road that is only useable during the winter when conditions allow for passage over lakes, rivers, and wetlands.
====  =========================  ==========

NID
^^^

A national unique identifier.

:Domain: A UUID.

    Example: 69822b23d217494896014e57a2edb8ac
:Data Type: Character (32)

Route Name English (1, 2, 3, 4)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The English version of a name of a particular route in a given road network as attributed by a national or sub
national agency. A particular Road Segment or Ferry Connection Segment can belong to more than one named route. In
such cases, it has multiple route name attributes.

:Domain: A complete English route name value such as "Trans-Canada Highway" or "None" when no value applies or
    "Unknown" when the value is not known.
:Data Type: Character (100)

Route Name French (1, 2, 3, 4)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The French version of a name of a particular route in a given road network as attributed by a national or sub national
agency. A particular Road Segment or Ferry Connection Segment can belong to more than one named route. In such cases,
it has multiple route name attributes.

:Domain: A complete French route name value such as "Autoroute transcanadienne" or "None" when no value applies or
    "Unknown" when the value is not known.
:Data Type: Character (100)

Route Number (1, 2, 3, 4, 5)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ID number of a particular route in a given road network as attributed by a national or sub-national agency. A
particular Road Segment or Ferry Connection Segment can belong to more than one numbered route. In such cases, it has
multiple route number attributes.

:Domain: A route number including possible associated non-integer characters such as "A" or "None" when no value applies.
    Examples: 1, 1A, 230-A, 430-28.
:Data Type: Character (100)

Object Metadata
^^^^^^^^^^^^^^^

Refer to the attributes describe in the section object metadata.

Junction
========

A feature that bounds a Road Element or a Ferry Connection. A Road Element or Ferry Connection always forms a
connection between two Junctions and, a Road Element or Ferry Connection is always bounded by exactly two Junctions.
A Junction Feature represents the physical connection between its adjoining Road Elements or Ferry Connections. A
Junction is defined at the intersection of three or more roads, at the junction of a road and a ferry, at the end of
a dead end road and at the junction of a road or ferry with a National, Provincial or Territorial Boundary.

:Is Abstract: No
:Geometry: Point

Attribute Section
-----------------

Exit Number
^^^^^^^^^^^

The ID number of an exit on a controlled access thoroughfare that has been assigned by an administrating body.

:Domain: An ID number including possible associated non-integer characters such as "A" or "None" when no value applies.
    Examples: 11, 11A, 11-A, 80-EST, 80-E, 80E.
:Data Type: Character (10)

Junction Type
^^^^^^^^^^^^^

The classification of a Junction.

:Domain:

====  =========================  ==========
Code  Label                      Definition
====  =========================  ==========
1     Intersection               An intersection between three or more Road Elements intersecting at same grade level.
2     DeadEnd                    A specific Junction that indicates that a Road Element ends and is not connected to any other Road Element or Ferry Connection.
3     Ferry                      A specific Junction that indicates that a Road Element connects to a Ferry Connection.
4     NatProvTer                 A specific Junction at the limit of a dataset indicating that a Road element or Ferry connection continues into the adjacent province, territory or country.
====  =========================  ==========

NID
^^^

A national unique identifier.

:Domain: A UUID.

    Example: 69822b23d217494896014e57a2edb8ac
:Data Type: Character (32)

Object Metadata
^^^^^^^^^^^^^^^

Refer to the attributes describe in the section object metadata.

Road Segment
============

A road is a linear section of the earth designed for or the result of vehicular movement. A Road Segment is the
specific representation of a portion of a road with uniform characteristics.

:Is Abstract: No
:Geometry: Line

Attribute Section
-----------------

Address Range Digitizing Direction Flag (left, right)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Indicates if the attribute event follows the same direction as the digitizing of the Road Element. A specific value
is defined for the left and right sides of the Road Element.

:Domain:

====  =========================  ==========
Code  Label                      Definition
====  =========================  ==========
1     Same Direction             Attribute event and Road Element geometry are in the same direction.
2     Opposite Direction         Attribute event and Road Element geometry are in opposite directions.
3     Not Applicable             Indication of the digitizing direction of the Road Element not needed for the attribute event.
====  =========================  ==========

Address Range NID
^^^^^^^^^^^^^^^^^

A UUID assigned to each particular block face address ranges.

:Domain: A UUID.

    Example: 69822b23d217494896014e57a2edb8ac
:Data Type: Character (32)

Closing Period
^^^^^^^^^^^^^^

The period in which the road or ferry connection is not available to the public.

:Domain:

====  =========================  ==========
Code  Label                      Definition
====  =========================  ==========
-1    Unknown                    It is not possible to determine if there is a closing period.
0     None                       There is no closing period. The road or ferry connection is open year round.
1     Summer                     Period of the year for which the absence of ice and snow prevent the access to the road or ferry connection.
2     Winter                     Period of the year for which ice and snow prevent the access to the road or ferry connection.
====  =========================  ==========

Exit Number
^^^^^^^^^^^

The ID number of an exit on a controlled access thoroughfare that has been assigned by an administrating body.

:Domain: An ID number including possible associated non-integer characters such as "A" or "None" when no value applies.
    Examples: 11, 11A, 11-A, 80-EST, 80-E, 80E.
:Data Type: Character (10)

First House Number (left, right)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The first house number address value along a particular side (left or right) of a Road Element. A specific value is
defined for the left and right sides of the Road Element.

:Domain: [-1..n] The value "0" is used when no value applies. The value "-1" is used when the value is unknown.
:Data Type: Integer

Functional Road Class
^^^^^^^^^^^^^^^^^^^^^

A classification based on the importance of the role that the Road Element or Ferry Connection performs in the
connectivity of the total road network.
:Domain:

====  =========================  ==========
Code  Label                      Definition
====  =========================  ==========
1     Freeway                    An unimpeded, high-speed controlled access thoroughfare for through traffic with typically no at- grade intersections, usually with no property access or direct access, and which is accessed by a ramp. Pedestrians are prohibited.
2     Expressway / Highway       A high-speed thoroughfare with a combination of controlled access intersections at any grade.
3     Arterial                   A major thoroughfare with medium to large traffic capacity.
4     Collector                  A minor thoroughfare mainly used to access properties and to feed traffic with right of way.
5     Local / Street             A low-speed thoroughfare dedicated to provide full access to the front of properties.
6     Local / Strata             A low-speed thoroughfare dedicated to provide access to properties with potential public restriction such as: trailer parks, First Nations, strata, private estates, seasonal residences.
7     Local / Unknown            A low-speed thoroughfare dedicated to provide access to the front of properties but for which the access regulations are unknown.
8     Alleyway / Lane            A low-speed thoroughfare dedicated to provide access to the rear of properties.
9     Ramp                       A system of interconnecting roadways providing for the controlled movement between two or more roadways.
10    Resource / Recreation      A narrow passage whose primary function is to provide access for resource extraction and may also have serve in providing public access to the backcountry.
11    Rapid Transit              A thoroughfare restricted to public transit buses.
12    Service Lane               A stretch of road permitting vehicles to come to a stop along a freeway or highway. Scale, service lane, emergency lane, lookout, and rest area.
13    Winter                     A road that is only useable during the winter when conditions allow for passage over lakes, rivers, and wetlands.
====  =========================  ==========

Last House Number (left, right)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The last house number address value along a particular side (left or right) of a Road Element. A specific value is
defined for the left and right sides of the Road Element.

:Domain: [-1..n] The value "0" is used when no value applies. The value "-1" is used when the value is unknown.
:Data Type: Integer

NID
^^^

A national unique identifier.

:Domain: A UUID.

    Example: 69822b23d217494896014e57a2edb8ac
:Data Type: Character (32)

Number of Lanes
^^^^^^^^^^^^^^^

The number of lanes existing on a Road Element.

:Domain: [1..8]
:Data Type: Integer

Official Place Name (left, right)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Official name of an administrative area, district or other named area which is required for uniqueness of the street name.

:Domain: Derived from the Street and place names table. A specific value is defined for the left and right sides of
    the Road Element. "None" when no value applies or "Unknown" when the value is not known.
:Data Type: Character (100)

Official Street Name Concatenated (left, right)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A concatenation of the officially recognized Directional prefix, Street type prefix, Street name article, Street name
body, Street type suffix, Directional suffix and Muni quadrant values.

:Domain: Derived from the Street and place names table. A specific value is defined for the left and right sides of
    the Road Element. "None" when no value applies or "Unknown" when the value is not known.
:Data Type: Character (100)

Paved Road Surface Type
^^^^^^^^^^^^^^^^^^^^^^^

The type of surface a paved Road Element has.

:Domain:

====  =========================  ==========
Code  Label                      Definition
====  =========================  ==========
-1    Unknown                    A paved road with an unknown surface type.
0     None                       No value applies.
1     Summer                     A paved road with a rigid surface such as concrete or steel decks.
2     Winter                     A paved road with a flexible surface such as asphalt or tar gravel.
3     Blocks                     A paved road with a surface made of blocks such as cobblestones.
====  =========================  ==========

Pavement Status
^^^^^^^^^^^^^^^

An indication of improvement applied to a Road surface.

:Domain:

====  =========================  ==========
Code  Label                      Definition
====  =========================  ==========
1     Paved                      A road with a surface made of hardened material such as concrete, asphalt, tar gravel, or steel decks.
2     Unpaved                    A road with a surface made of loose material such as gravel or dirt.
====  =========================  ==========

Road Jurisdiction
^^^^^^^^^^^^^^^^^

The agency with the responsibility/authority to ensure maintenance occurs but is not necessarily the one who
undertakes the maintenance directly.

:Domain: The Agency name or "None" when no value applies or "Unknown" when the value is not known.
:Data Type: Character (100)

Road Segment ID
^^^^^^^^^^^^^^^

A unique identifier within a dataset assigned to each Road Segment.

:Domain: [1..n]
:Data Type: Integer

Route Name English (1, 2, 3, 4)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The English version of a name of a particular route in a given road network as attributed by a national or sub
national agency. A particular Road Segment or Ferry Connection Segment can belong to more than one named route. In
such cases, it has multiple route name attributes.

:Domain: A complete English route name value such as "Trans-Canada Highway" or "None" when no value applies or
    "Unknown" when the value is not known.
:Data Type: Character (100)

Route Name French (1, 2, 3, 4)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The French version of a name of a particular route in a given road network as attributed by a national or sub national
agency. A particular Road Segment or Ferry Connection Segment can belong to more than one named route. In such cases,
it has multiple route name attributes.

:Domain: A complete French route name value such as "Autoroute transcanadienne" or "None" when no value applies or
    "Unknown" when the value is not known.
:Data Type: Character (100)

Route Number (1, 2, 3, 4, 5)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ID number of a particular route in a given road network as attributed by a national or sub-national agency. A
particular Road Segment or Ferry Connection Segment can belong to more than one numbered route. In such cases, it has
multiple route number attributes.

:Domain: A route number including possible associated non-integer characters such as "A" or "None" when no value applies.
    Examples: 1, 1A, 230-A, 430-28.
:Data Type: Character (100)

Speed Restriction
^^^^^^^^^^^^^^^^^

The maximum speed allowed on the road. The value is expressed in kilometers per hour

:Domain: -1 when unknown or a multiple of 5 lower than or equal to 120
:Data Type: Integer

Structure ID
^^^^^^^^^^^^

A national unique identifier assigned to the Road Segment or the set of adjoining Road Segments forming a structure.
This identifier allows for the reconstitution of a structure that is fragmented by Junctions.

:Domain: A UUID or "None" when no value applies.

    Example: 69822b23d217494896014e57a2edb8ac
:Data Type: Character (32)

Structure Name English
^^^^^^^^^^^^^^^^^^^^^^

The English version of the name of a road structure as assigned by a national or subnational agency.

:Domain: A complete structure name or "None" when no value applies or "Unknown" when the structure name is not known.
:Data Type: Character (100)

Structure Name French
^^^^^^^^^^^^^^^^^^^^^

The French version of the name of a road structure as assigned by a national or subnational agency.

:Domain: A complete structure name or "None" when no value applies or "Unknown" when the structure name is not known.
:Data Type: Character (100)

Structure Type
^^^^^^^^^^^^^^

The classification of a structure.

:Domain:

====  =========================  ==========
Code  Label                      Definition
====  =========================  ==========
0     None                       No value applies.
1     Bridge                     A manmade construction that supports a road on a raised structure and spans an obstacle, river, another road, or railway.
2     Bridge covered             A manmade construction that supports a road on a covered raised structure and spans an obstacle, river, another road, or railway.
3     Bridge moveable            A manmade construction that supports a road on a moveable raised structure and spans an obstacle, river, another road, or railway.
4     Bridge unknown             A bridge for which it is currently impossible to determine whether its structure is covered, moveable or other.
5     Tunnel                     An enclosed manmade construction built to carry a road through or below a natural feature or other obstructions.
6     Snowshed                   A manmade roofed structure built over a road in mountainous areas to prevent snow slides from blocking the road.
7     Dam                        A manmade linear structure built across a waterway or floodway to control the flow of water and supporting a road for motor vehicles.
====  =========================  ==========

Traffic Direction
^^^^^^^^^^^^^^^^^

The direction(s) of traffic flow allowed on the road.

:Domain:

====  =========================  ==========
Code  Label                      Definition
====  =========================  ==========
-1    Unknown                    Information not acquired.
1     Both directions            Traffic flow is allowed in both directions.
2     Same direction             The direction of one way traffic flow is the same as the digitizing direction of the Road Segment.
3     Opposite direction         The direction of one way traffic flow is opposite to the digitizing direction of the Road Segment.
====  =========================  ==========

Unpaved Road Surface Type
^^^^^^^^^^^^^^^^^^^^^^^^^

The type of surface an unpaved Road Element has.

:Domain:

====  =========================  ==========
Code  Label                      Definition
====  =========================  ==========
-1    Unknown                    An unpaved road for which the characteristics of the material used is not known.
0     None                       No value applies.
1     Gravel                     A dirt road whose surface has been improved by grading with gravel.
2     Dirt                       Roads whose surface is formed by the removal of vegetation and/or by the transportation movements over that road which inhibit further growth of any vegetation.
====  =========================  ==========

Object Metadata
^^^^^^^^^^^^^^^

Refer to the attributes describe in the section object metadata.

Street and Place Names
======================

A street name recognized by the municipality or naming authority and a name of an administrative area, district or
other named area which is required for uniqueness of the street name.

:Is Abstract: No
:Geometry:

Attribute Section
-----------------

Directional Prefix
^^^^^^^^^^^^^^^^^^

A geographic direction that is part of the street name and precedes the street name body or, if appropriate, the
street type prefix.

:Domain:

====  =========================  ==========
Code  Label                      Definition
====  =========================  ==========
0     None                       No value applies.
1     North
2     Nord
3     South
4     Sud
5     East
6     Est
7     West
8     Ouest
9     Northwest
10    Nord-ouest
11    Northeast
12    Nord-est
13    Southwest
14    Sud-ouest
15    Southeast
16    Sud-est
17    Central
18    Centre
====  =========================  ==========

Directional Suffix
^^^^^^^^^^^^^^^^^^

A geographic direction that is part of the street name and succeeds the street name body or, if appropriate, the
street type suffix.

:Domain:

====  =========================  ==========
Code  Label                      Definition
====  =========================  ==========
0     None                       No value applies.
1     North
2     Nord
3     South
4     Sud
5     East
6     Est
7     West
8     Ouest
9     Northwest
10    Nord-ouest
11    Northeast
12    Nord-est
13    Southwest
14    Sud-ouest
15    Southeast
16    Sud-est
17    Central
18    Centre
====  =========================  ==========

Muni Quadrant
^^^^^^^^^^^^^

The attribute Muni quadrant is used in some addresses much like the directional attributes where the town is divided
into sections based on major east-west and north- south divisions. The effect is as if multiple directional were used.

:Domain:

====  =========================  ==========
Code  Label                      Definition
====  =========================  ==========
0     None                       No value applies.
1     South-West
2     South-East
3     North-East
4     North-West
====  =========================  ==========

NID
^^^

A national unique identifier.

:Domain: A UUID.

    Example: 69822b23d217494896014e57a2edb8ac
:Data Type: Character (32)

Place Name
^^^^^^^^^^

Name of an administrative area, district or other named area which is required for uniqueness of the street name.

:Domain: The complete name of the place.
    Examples: Arnold's Cove, Saint-Jean-Baptiste-de-l'Îsle-Verte, Sault Ste. Marie, Grand- Sault, Grand Falls.
:Data Type: Character (100)

Place Type
^^^^^^^^^^

Expression specifying the type of place.

:Domain:

====================================================================  ==========
Label                                                                 Definition
====================================================================  ==========
None                                                                  No value applies.
Borough / Borough
Chartered Community
City / Cité
City / Ville
Community / Communauté
County (Municipality) / Comté (Municipalité)
Cree Village / Village Cri
Crown Colony / Colonie de la couronne
District (Municipality) / District (Municipalité)
Hamlet / Hameau Improvement District
Indian Government District
Indian Reserve / Réserve indienne
Indian Settlement / Établissement indien
Island Municipality
Local Government District
Lot / Lot
Municipal District / District municipal
Municipality / Municipalité
Naskapi Village / Village Naskapi
Nisga'a land / Terre Nisga'a
Nisga'a Village / Village Nisga'a
Northern Hamlet / Hameau nordique
Northern Town / Ville nordique
Northern Village / Village nordique
Parish (Municipality) / Paroisse (Municipalité)
Parish / Paroisse Region / Région
Regional District Electoral Area
Regional Municipality / Municipalité régionale
Resort Village / Centre de villégiature
Rural Community
Rural Municipality / Municipalité rurale
Settlement / Établissement
Special Area
Specialized Municipality / Municipalité spécialisée
Subdivision of County Municipality
Subdivision of Regional District
Subdivision of Unorganized
Summer Village / Village estival
Terre inuite
Terres réservées
Teslin land / Terre Teslin
Town / Ville
Township (Municipality) / Canton (Municipalité)
Township / Canton
United Township (Municipality) / Cantons- unis (Municipalité)
Unorganized / Non- organisé
Village / Village
Without Designation (Municipality) / Sans désignation (Municipalité)
====================================================================  ==========

Province
------------

Province or Territory covered by the dataset.

:Domain:

====  =====
Code  Label
====  =====
1     Newfoundland and Labrador
2     Nova Scotia
3     Prince Edward Island
4     New Brunswick
5     Quebec
6     Ontario
7     Manitoba
8     Saskatchewan
9     Alberta
10    British Columbia
11    Yukon Territory
12    Northwest Territories
13    Nunavut
====  =====

Street Name Article
^^^^^^^^^^^^^^^^^^^

Article(s) that is (are) part of the street name and located at the beginning.

:Domain:

======================== ==========
Label                    Definition
======================== ==========
None                     No value applies.
à
à l'
à la
au aux by the chez d'
de
de l' de la des du
l' la
le
les
of the the
======================== ==========

Street Name Body
^^^^^^^^^^^^^^^^

The portion of the street name (either official or alternate) that has the most identifying power excluding street
type and directional prefixes or suffixes and street name articles.

:Domain: The complete street name body or "None" when no value applies.

    Examples: Capitale, Trésor, Golf, Abbott, Abbott's, Main, Church, Park, Bread and Cheese.
:Data Type: Character (100)

Street Type Prefix
^^^^^^^^^^^^^^^^^^

A part of the street name of a Road Element identifying the street type. A prefix precedes the street name body of
a Road Element.

:Domain: Listed values are incomplete. "None" when no value applies or "Unknown" when the value is not known.
:Data Type: Character (30)

======================== ==========
Label                    Definition
======================== ==========
None                     No value applies.
Abbey
Access
Acres
Aire
Allée
Alley
Autoroute
Avenue
Barrage
Bay
Beach
Bend
Bloc
Block
Boulevard
Bourg
Brook
By-pass
Byway
Campus
Cape
Carre
Carrefour
Centre
Cercle
Chase
Chemin
Circle
Circuit
Close
Common
Concession
Corners
Côte
Cour
Court
Cove
Crescent
Croft
Croissant
Crossing
Crossroads
Cul-de-sac
Dale
Dell
Desserte
Diversion
Downs
Drive
Droit de passage
Échangeur
End
Esplanade
Estates
Expressway
Extension
Farm
Field
Forest
Front
Gardens
Gate
Glade
Glen
Green
Grounds
Grove
Harbour
Haven
Heath
Heights
Highlands
Highway
Hill
Hollow
Île
Impasse
Island
Key
Knoll
Landing
Lane
Laneway
Limits
Line
Link
Lookout
Loop
Mall
Manor
Maze
Meadow
Mews
Montée
Moor
Mount
Mountain
Orchard
Parade
Parc
Park
Parkway
Passage
Path
Pathway
Peak
Pines
Place
Plateau
Plaza
Point
Port
Private
Promenade
Quay
Rang
Range
Reach
Ridge
Right of Way
Rise
Road
Rond Point
Route
Row
Rue
Ruelle
Ruisseau
Run
Section
Sentier
Sideroad
Square
Street
Stroll
Subdivision
Terrace
Terrasse
Thicket
Towers
Townline
Trace
Trail
Trunk
Turnabout
Vale
Via
View
Village
Vista
Voie
Walk
Way
Wharf
Wood
Woods
Wynd
======================== ==========

Street Type Suffix
^^^^^^^^^^^^^^^^^^

A part of the street name of a Road Element identifying the street type. A suffix follows the street name body of
a Road Element.

:Domain: Same domain as the attribute street type prefix. "None" when no value applies or "Unknown" when the value
    is not known.
:Data Type: Character (30)

Object Metadata
^^^^^^^^^^^^^^^

Refer to the attributes describe in the section object metadata.

Toll Point
==========

Place where a right-of-way is charged to gain access to a motorway, a bridge, etc.

:Is Abstract: No
:Geometry: Point

Attribute Section
-----------------

NID
^^^

A national unique identifier.

:Domain: A UUID.

    Example: 69822b23d217494896014e57a2edb8ac
:Data Type: Character (32)

Road Element NID
^^^^^^^^^^^^^^^^

The NID of the Road Element on which the point geometry is located.

:Domain: A UUID.

    Example: 69822b23d217494896014e57a2edb8ac
:Data Type: Character (32)

Toll Point Type
^^^^^^^^^^^^^^^

The type of toll point.

:Domain:

====  =========================  ==========
Code  Label                      Definition
====  =========================  ==========
-1    Unknown                    A toll point for which it is currently impossible to determine the specific type.
1     Physical Toll Booth        A toll booth is a construction along or across the road where toll can be paid to employees of the organization in charge of collecting the toll, to machines capable of automatically recognizing coins or bills or to machines involving electronic methods of payment like credit cards or bank cards.
2     Virtual Toll Booth         At a virtual point of toll payment, toll will be charged via automatic registration of the passing vehicle by subscription or invoice.
3     Hybrid                     Hybrid signifies a toll booth which is both physical and virtual.
====  =========================  ==========

Object Metadata
^^^^^^^^^^^^^^^

Refer to the attributes describe in the section object metadata.