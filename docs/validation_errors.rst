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

E00101
------

:Validation: Duplicated lines.
:Description: Identify duplicated line features.

E00201
------

:Validation: Duplicated points.
:Description: Identify duplicated point features.

E00301
------

:Validation: Isolated lines.
:Description: Identify line features which are not connected to any other line features.

E004
----

:Validation: Dates.

E00401
^^^^^^

:Description: ?

E00402
^^^^^^

:Description: ?

E00403
^^^^^^

:Description: ?

E00404
^^^^^^

:Description: ?

E00405
^^^^^^

:Description: ?

E00406
^^^^^^

:Description: ?

E00407
^^^^^^

:Description: ?

E00501
------

:Validation: Deadend proximity.
:Description: ?

E00601
------

:Validation: Conflicting exit numbers.
:Description: ?

E00701
------

:Validation: Exit number - road class relationship.
:Description: ?

E008
----

:Validation: Ferry-road connectivity.

E00801
^^^^^^

:Description: ?

E00802
^^^^^^

:Description: ?

E009
----

:Validation: Identifiers.

E00901
^^^^^^

:Description: ?

E00902
^^^^^^

:Description: ?

E00903
^^^^^^

:Description: ?

E00904
^^^^^^

:Description: ?

E01001
------

:Validation: Line endpoint clustering.
:Description: ?

E01101
------

:Validation: Line length.
:Description: ?

E01201
------

:Validation: Line merging angle.
:Description: ?

E01301
------

:Validation: Line proximity.
:Description: ?

E01401
------

:Validation: Number of lanes.
:Description: ?

E01501
------

:Validation: NID Linkages.
:Description: ?

E016
----

:Validation: Conflicting pavement status.

E01601
^^^^^^

:Description: ?

E01602
^^^^^^

:Description: ?

E01603
^^^^^^

:Description: ?

E01604
^^^^^^

:Description: ?

E01701
------

:Validation: Point proximity.
:Description: ?

E018
----

:Validation: Structures.

E01801
^^^^^^

:Description: ?

E01802
^^^^^^

:Description: ?

E01803
^^^^^^

:Description: ?

E01804
^^^^^^

:Description: ?

E01901
------

:Validation: Road class - route number relationship.
:Description: ?

E02001
------

:Validation: Self-intersecting road elements.
:Description: ?

E02101
------

:Validation: Self-intersecting structures.
:Description: ?

E02201
------

:Validation: Route contiguity.
:Description: ?

E023
----

:Validation: Speed.

E02301
^^^^^^

:Description: ?

E02302
^^^^^^

:Description: ?
