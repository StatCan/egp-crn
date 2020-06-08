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

    :Structure: E (Fixed Letter) || Major Error Code || Minor Error Code || NRN Dataset
    :Format: E ### ## DD
    :Example: E00103RS

NRN Datasets
------------

The "NRN Dataset" value indicates which dataset the errors pertain to, it does not alter the meaning of the error code.
If a validation operates on multiple datasets simultaneously, the first dataset in list of datasets passed to the
validation will be used.

Possible values are as follows:

* AR: addrange
* AL: altnamlink
* BP: blkpassage
* FS: ferryseg
* JC: junction
* RS: roadseg
* SP: strplaname
* TP: tollpoint

Errors
======

E00101
------

:Validation: Duplicated lines.
:Description: Line segments within the same dataset must not be duplicated.

.. figure:: /_static/figures/e00101.png
    :alt: Example Image

    Figure E00101: Duplicated lines.

E00201
------

:Validation: Duplicated points.
:Description: Points within the same dataset must not be duplicated.

.. figure:: /_static/figures/e00201.png
    :alt: Example Image

    Figure E00201: Duplicated points.

E00301
------

:Validation: Isolated lines.
:Description: Line segments must be connected to at least one other line segment.

.. figure:: /_static/figures/e00301.png
    :alt: Example Image

    Figure E00301: Isolated lines.

E004
----

:Validation: Dates.

E00401
^^^^^^

:Description: Attributes "credate" and "revdate" must be numeric.

.. figure:: /_static/figures/e00401.png
    :alt: Example Image

    Figure E00401: Dates.

E00402
^^^^^^

:Description: Attributes "credate" and "revdate" must have lengths of 4, 6, or 8. Therefore, using zero-padded digits,
    dates can represent a year, year + month, or year + month + day.

.. figure:: /_static/figures/e00402.png
    :alt: Example Image

    Figure E00402: Dates.

E00403
^^^^^^

:Description: Attributes "credate" and "revdate" must have a year (first 4 digits) between 1960 and the current year,
    inclusively.

.. figure:: /_static/figures/e00403.png
    :alt: Example Image

    Figure E00403: Dates.

E00404
^^^^^^

:Description: Attributes "credate" and "revdate" must have a month (digits 5 and 6) between 01 and 12, inclusively.

.. figure:: /_static/figures/e00404.png
    :alt: Example Image

    Figure E00404: Dates.

E00405
^^^^^^

:Description: Attributes "credate" and "revdate" must have a day (digits 7 and 8) between 01 and the monthly maximum,
    inclusively.

.. figure:: /_static/figures/e00405.png
    :alt: Example Image

    Figure E00405: Dates.

E00406
^^^^^^

:Description: Attributes "credate" and "revdate" must be <= today.

.. figure:: /_static/figures/e00406.png
    :alt: Example Image

    Figure E00406: Dates.

E00407
^^^^^^

:Description: Attribute "credate" must be <= attribute "revdate".

.. figure:: /_static/figures/e00407.png
    :alt: Example Image

    Figure E00407: Dates.

E00501
------

:Validation: Dead End proximity.
:Description: Junctions with attribute "junctype" equal to "Dead End" must be >= 5 meters from disjointed line segments.

.. figure:: /_static/figures/e00501.png
    :alt: Example Image

    Figure E00501: Dead End proximity.

E00601
------

:Validation: Conflicting exit numbers.
:Description: Attribute "exitnbr" must be identical or the default value for all road segments constituting a
    road element.

.. figure:: /_static/figures/e00601.png
    :alt: Example Image

    Figure E00601: Conflicting exit numbers.

E00701
------

:Validation: Exit number - road class relationship.
:Description: When attribute "exitnbr" is not equal to the default value, attribute "roadclass" must equal one of the
    following: "Ramp", "Service Lane".

.. figure:: /_static/figures/e00701.png
    :alt: Example Image

    Figure E00701: Exit number - road class relationship.

E008
----

:Validation: Ferry - road connectivity.

E00801
^^^^^^

:Description: Ferry segments must be connected to a road segment at at least one endpoint.

.. figure:: /_static/figures/e00801.png
    :alt: Example Image

    Figure E00801: Ferry - road connectivity.

E00802
^^^^^^

:Description: Ferry segments cannot be connected to multiple road segments at the same endpoint.

.. figure:: /_static/figures/e00802.png
    :alt: Example Image

    Figure E00802: Ferry - road connectivity.

E009
----

:Validation: Identifiers.

E00901
^^^^^^

:Description: IDs must be 32 digits in length.

.. figure:: /_static/figures/e00901.png
    :alt: Example Image

    Figure E00901: Identifiers.

E00902
^^^^^^

:Description: IDs must be hexadecimal.

.. figure:: /_static/figures/e00902.png
    :alt: Example Image

    Figure E00902: Identifiers.

E00903
^^^^^^

:Description: IDs in UUID attribute columns must be unique.

.. figure:: /_static/figures/e00903.png
    :alt: Example Image

    Figure E00903: Identifiers.

E00904
^^^^^^

:Description: IDs in UUID attribute column must not be the default value.

.. figure:: /_static/figures/e00904.png
    :alt: Example Image

    Figure E00904: Identifiers.

E01001
------

:Validation: Line endpoint clustering.
:Description: Line segments must have <= 3 points within 83 meters of either endpoint, inclusively.

.. figure:: /_static/figures/e01001.png
    :alt: Example Image

    Figure E01001: Line endpoint clustering.

E01101
------

:Validation: Line length.
:Description: Line segments must be >= 2 meters in length.

.. figure:: /_static/figures/e01101.png
    :alt: Example Image

    Figure E01101: Line length.

E01201
------

:Validation: Line merging angle.
:Description: Line segments must only merge at angles >= 40 degrees.

.. figure:: /_static/figures/e01201.png
    :alt: Example Image

    Figure E01201: Line merging angle.

E01301
------

:Validation: Line proximity.
:Description: Line segments must be >= 3 meters from each other, excluding connected segments.

.. figure:: /_static/figures/e01301.png
    :alt: Example Image

    Figure E01301: Line proximity.

E01401
------

:Validation: Number of lanes.
:Description: Attribute "nbrlanes" must be between 1 and 8, inclusively.

.. figure:: /_static/figures/e01401.png
    :alt: Example Image

    Figure E01401: Number of lanes.

E01501
------

:Validation: NID linkages.
:Description: ID(s) from the specified attribute column are not present in the linked dataset's "NID" attribute column.

.. figure:: /_static/figures/e01501.png
    :alt: Example Image

    Figure E01501: NID linkages.

E016
----

:Validation: Conflicting pavement status.

E01601
^^^^^^

:Description: Attribute "pavsurf" cannot equal "None" when attribute "pavstatus" equals "Paved".

.. figure:: /_static/figures/e01601.png
    :alt: Example Image

    Figure E01601: Conflicting pavement status.

E01602
^^^^^^

:Description: Attribute "unpavsurf" must equal "None" when attribute "pavstatus" equals "Paved".

.. figure:: /_static/figures/e01602.png
    :alt: Example Image

    Figure E01602: Conflicting pavement status.

E01603
^^^^^^

:Description: Attribute "pavsurf" must equal "None" when attribute "pavstatus" equals "Unpaved".

.. figure:: /_static/figures/e01603.png
    :alt: Example Image

    Figure E01603: Conflicting pavement status.

E01604
^^^^^^

:Description: Attribute "unpavsurf" cannot equal "None" when attribute "pavstatus" equals "Unpaved".

.. figure:: /_static/figures/e01604.png
    :alt: Example Image

    Figure E01604: Conflicting pavement status.

E01701
------

:Validation: Point proximity.
:Description: Points must be >= 3 meters from each other.

.. figure:: /_static/figures/e01701.png
    :alt: Example Image

    Figure E01701: Point proximity.

E018
----

:Validation: Structure attributes.

E01801
^^^^^^

:Description: Dead end road segments must have attribute "structtype" equal to "None" or the default value.

.. figure:: /_static/figures/e01801.png
    :alt: Example Image

    Figure E01801: Structure attributes.

E01802
^^^^^^

:Description: Structures must be contiguous (i.e. all line segments must be touching). The specified structure
    represents all geometries where attribute "structid" equals the specified structure ID.

.. figure:: /_static/figures/e01802.png
    :alt: Example Image

    Figure E01802: Structure attributes.

E01803
^^^^^^

:Description: Attribute "structid" must be identical and not the default value for all line segments constituting a
    contiguous structure (i.e. all connected line segments where attribute "structtype" is not equal to the default
    value).

.. figure:: /_static/figures/e01803.png
    :alt: Example Image

    Figure E01803: Structure attributes.

E01804
^^^^^^

:Description: Attribute "structtype" must be identical and not the default value for all line segments constituting a
    contiguous structure (i.e. all connected line segments where attribute "structtype" is not equal to the default
    value).

.. figure:: /_static/figures/e01804.png
    :alt: Example Image

    Figure E01804: Structure attributes.

E01901
------

:Validation: Road class - route number relationship.
:Description: Attribute "rtnumber1" cannot equal the default value when attribute "roadclass" equals one of the
    following: "Expressway / Highway", "Freeway".

.. figure:: /_static/figures/e01901.png
    :alt: Example Image

    Figure E01901: Road class - route number relationship.

E02001
------

:Validation: Self-intersecting road elements.
:Description: Road segments which constitute a self-intersecting road element must have attribute "roadclass" equal to
    one of the following: "Expressway / Highway", "Freeway", "Ramp", "Rapid Transit".

.. figure:: /_static/figures/e02001.png
    :alt: Example Image

    Figure E02001: Self-intersecting road elements.

E02101
------

:Validation: Self-intersecting structures.
:Description: Line segments which intersect themselves must have a "structtype" attribute not equal to "None".

.. figure:: /_static/figures/e02101.png
    :alt: Example Image

    Figure E02101: Self-intersecting structures.

E02201
------

:Validation: Route contiguity.
:Description: Routes must be contiguous (i.e. all line segments must be touching). The specified route represents all
    geometries where one of the specified route name attributes equals the specified route name.

.. figure:: /_static/figures/e02201.png
    :alt: Example Image

    Figure E02201: Route contiguity.

E023
----

:Validation: Speed.

E02301
^^^^^^

:Description: Attribute "speed" must be between 5 and 120, inclusively.

.. figure:: /_static/figures/e02301.png
    :alt: Example Image

    Figure E02301: Speed.

E02302
^^^^^^

:Description: Attribute "speed" must be a multiple of 5.

.. figure:: /_static/figures/e02302.png
    :alt: Example Image

    Figure E02302: Speed.
