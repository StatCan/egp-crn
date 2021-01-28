*********************
NRN Validation Errors
*********************

.. contents::
   :depth: 3

Abbreviations
=============

.. glossary::
    NID
        National Unique Identifier

    NRCan
        Natural Resources Canada

    NRN
        National Road Network

    StatCan
        Statistics Canada

    UUID
        Universal Unique Identifier

Overview
========

The NRN implements several validations against the attributes and geometry of constituent datasets. Most of the
currently implemented validations were adopted from the NRCan NRN process while a few are StatCan additions. Some
validations may be removed or modified after consultation with GeoBase partners as required. It is important to note
that not all validation failures are truly errors, but rather flags against potential errors which is up to the data
provider to review. Reference to such validations and the associated data records as errors is only for consistency and
simplicity of references.

Error Code Structure
====================

All validations have been assigned a unique error code with the following composition:

    :Structure: E (Fixed Letter) || Major Error Code || Minor Error Code
    :Format: E ### ##
    :Example: E00103

Errors
======

E001
----

:Validation: Duplicated lines.

E00101
^^^^^^

:Description: Line segments within the same dataset must not be duplicated.

E00102
^^^^^^

:Description: Line segments must not contain repeated adjacent coordinates.

E00103
^^^^^^

:Description: Line segments within the same dataset must not overlap (i.e. contain duplicated adjacent points).

E00201
------

:Validation: Duplicated points.
:Description: Points within the same dataset must not be duplicated.

E003
----

:Validation: Isolated lines.

E00301
^^^^^^

:Description: Line segments must be connected to at least one other line segment.

E00302
^^^^^^

:Description: Line segments must only connect at endpoint vertices.

E004
----

:Validation: Dates.

E00401
^^^^^^

:Description: Attributes "credate" and "revdate" must be numeric.

E00402
^^^^^^

:Description: Attributes "credate" and "revdate" must have lengths of 4, 6, or 8. Therefore, using zero-padded digits,
    dates can represent a year, year + month, or year + month + day.

E00403
^^^^^^

:Description: Attributes "credate" and "revdate" must have a year (first 4 digits) between 1960 and the current year,
    inclusively.

E00404
^^^^^^

:Description: Attributes "credate" and "revdate" must have a month (digits 5 and 6) between 01 and 12, inclusively.

E00405
^^^^^^

:Description: Attributes "credate" and "revdate" must have a day (digits 7 and 8) between 01 and the monthly maximum,
    inclusively.

E00406
^^^^^^

:Description: Attributes "credate" and "revdate" must be <= today.

E00407
^^^^^^

:Description: Attribute "credate" must be <= attribute "revdate".

E00501
------

:Validation: Dead End proximity.
:Description: Junctions with attribute "junctype" equal to "Dead End" must be >= 5 meters from disjointed line segments.

E00601
------

:Validation: Conflicting exit numbers.
:Description: Attribute "exitnbr" must be identical or the default value or "None" for all road segments constituting a
    road element.

E00701
------

:Validation: Exit number - road class relationship.
:Description: When attribute "exitnbr" is not equal to the default value or "None", attribute "roadclass" must equal
    one of the following: "Expressway / Highway", "Freeway", "Ramp", "Rapid Transit", "Service Lane".

E008
----

:Validation: Ferry - road connectivity.

E00801
^^^^^^

:Description: Ferry segments must be connected to a road segment at at least one endpoint.

E00802
^^^^^^

:Description: Ferry segments cannot be connected to multiple road segments at the same endpoint.

E009
----

:Validation: Identifiers.

E00901
^^^^^^

:Description: IDs must be 32 digits in length.

E00902
^^^^^^

:Description: IDs must be hexadecimal.

E00903
^^^^^^

:Description: IDs in UUID attribute columns must be unique.

E00904
^^^^^^

:Description: IDs in UUID attribute column must not equal "None" nor the default value.

E01001
------

:Validation: Line internal clustering.
:Description: Line segments must have >= 1 meter distance between adjacent coordinates.

E01101
------

:Validation: Line length.
:Description: Line segments must be >= 5 meters in length.

E01201
------

:Validation: Line merging angle.
:Description: Line segments must only merge at angles >= 15 degrees.

E01301
------

:Validation: Line proximity.
:Description: Line segments must be >= 5 meters from each other, excluding connected segments.

E01401
------

:Validation: Number of lanes.
:Description: Attribute "nbrlanes" must be between 1 and 8, inclusively.

E01501
------

:Validation: NID linkages.
:Description: ID(s) from the specified attribute column are not present in the linked dataset's "NID" attribute column.

E016
----

:Validation: Conflicting pavement status.

E01601
^^^^^^

:Description: Attribute "pavsurf" cannot equal "None" when attribute "pavstatus" equals "Paved".

E01602
^^^^^^

:Description: Attribute "unpavsurf" must equal "None" when attribute "pavstatus" equals "Paved".

E01603
^^^^^^

:Description: Attribute "pavsurf" must equal "None" when attribute "pavstatus" equals "Unpaved".

E01604
^^^^^^

:Description: Attribute "unpavsurf" cannot equal "None" when attribute "pavstatus" equals "Unpaved".

E01701
------

:Validation: Point proximity.
:Description: Points must be >= 5 meters from each other.

E018
----

:Validation: Structure attributes.

E01801
^^^^^^

:Description: Dead end road segments must have attribute "structtype" equal to "None" or the default value.

E01802
^^^^^^

:Description: Structures must be contiguous (i.e. all line segments must be touching). The specified structure
    represents all geometries where attribute "structid" equals the specified structure ID.

E01803
^^^^^^

:Description: Attribute "structid" must be identical and not the default value for all line segments constituting a
    contiguous structure (i.e. all connected line segments where attribute "structtype" is not equal to the default
    value).

E01804
^^^^^^

:Description: Attribute "structtype" must be identical and not the default value for all line segments constituting a
    contiguous structure (i.e. all connected line segments where attribute "structtype" is not equal to the default
    value).

E01901
------

:Validation: Road class - route number relationship.
:Description: Attribute "rtnumber1" cannot equal the default value or "None" when attribute "roadclass" equals one of
    the following: "Expressway / Highway", "Freeway".

E02001
------

:Validation: Self-intersecting road elements.
:Description: Road segments which constitute a self-intersecting road element must have attribute "roadclass" equal to
    one of the following: "Expressway / Highway", "Freeway", "Ramp", "Rapid Transit", "Service Lane".

E02101
------

:Validation: Self-intersecting structures.
:Description: Line segments which intersect themselves must have a "structtype" attribute not equal to "None".

E02201
------

:Validation: Route contiguity.
:Description: Routes must be contiguous (i.e. all line segments must be touching). The specified route represents all
    geometries where one of the specified route name attributes equals the specified route name.

E023
----

:Validation: Speed.

E02301
^^^^^^

:Description: Attribute "speed" must be between 5 and 120, inclusively.

E02302
^^^^^^

:Description: Attribute "speed" must be a multiple of 5.

E02401
------

:Validation: Encoding.
:Description: Attribute contains one or more question mark ("?"), which may be the result of invalid character encoding.

E02501
------

:Validation: Out-of-scope.
:Description: Geometry is partially or completely outside of the target area, based on Census provincial / territorial boundaries.
