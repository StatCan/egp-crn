********************************************
Data Product Specifications - Segmented View
********************************************

.. include :: <isonum.txt>

.. note::
    These specifications are produced in accordance with International Standard ISO/TC 211, 19131: 2007 Geographic 
    Information / Geomatics – Data Product Specification, which refers in particular to standard ISO 19115: 2003 
    Geographic information – Metadata.

.. contents::
   :depth: 3

Overview
========

Title
-----

National Road Network

Reference date
--------------

Creation date of the data product specifications: 2007-05-31

Responsible party
-----------------

GeoBase

Statistics Canada

Statistical Geomatics Centre 

170, Tunney’s Pasture Driveway, 

Ottawa (Ontario), Canada

K1A 0T6

Telephone: 1-800-263-1136

Fax: 1-514-283-9350

E-mail: STATCAN.infostats-infostats.STATCAN@canada.ca

Website: http://www.geobase.ca 

Language
--------

Languages in which the data product specifications are available in accordance with the ISO 639-2 standard:
eng – english
fra – french

Terms and definitions
---------------------

Attribute
    Characteristic of a feature. For example, number of lanes or pavement status.

Dataset
    Data collection identifiable for a Canadian Province or Territory.

Entity
    Digital representation of a real world phenomenon. For example, the digital representation of King Street is an entity.

Object
    An object is an instance of a class.

Abbreviations and acronyms
--------------------------

.. glossary::
    CRSID
        Coordinate Reference System Identifier CCOG Canadian Council on Geomatics
    
    GDF
        Geographic Data File
    
    GML 
        Geography Markup Language
    
    GPKG 
        Open Geospatial Consortium Geopackage
    
    GPS 
        Global Positioning System
    
    IACG 
        Inter-Agency Committee on Geomatics
    
    ID 
        Identifier
    
    ISO 
        International Organization for Standardization
    
    KML 
        Keyhole Markup Language
    
    NAD83CSRS 
        North American Datum 1983 (Canadian Spatial Reference System) NID National Identifier
    
    NRCan 
        Natural Resources Canada
    
    NRN 
        National Road Network
    
    OGC 
        Open Geospatial Consortium
    
    TC
        Technical Committee
    
    XML
        Extensible Markup Language

Informal description of the data product
----------------------------------------

The National Road Network (NRN) product contains quality geospatial data (current, accurate, consistent and maintained) 
of Canadian road phenomena. The NRN product is distributed in the form of thirteen provincial or territorial datasets 
and consists of two linear entities (Road Segment and Ferry Connection Segment) and three punctual entities (Junction, 
Blocked Passage, Toll Point) with which is associated a series of descriptive attributes such as, among others: First 
House Number, Last House Number, Street Name Body, Place Name, Functional Road Class, Pavement Status, Number Of Lanes, 
Structure Type, Route Number, Route Name, Exit Number.

The maintenance of the NRN data is done within the framework of intergovernmental partnership agreements (federal, 
provincial, territorial and municipal) by interested closest to source governmental bodies capable of offering adequate 
and current representations of the road phenomena. The frequency of maintenance aimed is of at least one update a year. 
Data produced form a homogeneous and standardized view of the entire Canadian territory.

The NRN conceptual model was elaborated in collaboration with interested data providers and is adopted by the Canadian 
Council on Geomatics (CCOG). The standard ISO 14825 — *Intelligent transport systems — Geographic Data Files (GDF) — 
Overall data specification* served as a guide for the elaboration of the NRN conceptual model and catalogue. The NRN 
vocabulary used (class names, attribute names and definitions) largely conforms to the ISO 14825.

Specification Scope
===================

This section describes the scope referred to by information provided in subsequent sections which describe the product.

Scope identification
--------------------

Global

.. note::
    "Global" means that this scope refers to all parts of this data product specifications.

Level
-----

This scope refers to the following level according to the ISO 19115 standard and CAN/CGSB - 171.100- 2009 standards:
006 - series

Level name
----------

NRN

Extent
------

This section describes the spatial and temporal extent of the scope.

Description
^^^^^^^^^^^

Canadian landmass

NRN data are seamless between datasets and form a continuous network over the Canadian landmass.

Vertical extent
^^^^^^^^^^^^^^^

The NRN data are two-dimensional. There is no elevation (z) associated with the data.

Minimum value
"""""""""""""

Not applicable

Maximum value
"""""""""""""

Not applicable

Unit of measure
"""""""""""""""

Not applicable

Vertical datum
""""""""""""""

Not applicable

Horizontal extent
^^^^^^^^^^^^^^^^^

The geographic extent is given by the following geographic bounding box:

West bound longitude
""""""""""""""""""""

-141.0

East bound longitude
""""""""""""""""""""

-52.6

South bound latitude
""""""""""""""""""""

+41.7

North bound latitude
""""""""""""""""""""

+76.5

Temporal extent
^^^^^^^^^^^^^^^

The temporal extent is given by the following period of time:

Beginning date
""""""""""""""

1979-07

Ending date
"""""""""""

Today

.. note::
    "Today" means the current date of publication of an instance of the NRN. That is, an instance of the NRN may include 
    the road network that is current at the time of publication.

Data Product identification
===========================

Title
-----

National Road Network

Alternate title
---------------

NRN

Abstract
--------

The NRN product is distributed in the form of thirteen provincial or territorial datasets and consists of two linear 
entities (Road Segment and Ferry Connection Segment) and three punctual entities (Junction, Blocked Passage, Toll 
Point) with which is associated a series of descriptive attributes such as, among others: First House Number, Last 
House Number, Street Name Body, Place Name, Functional Road Class, Pavement Status, Number Of Lanes, Structure Type, 
Route Number, Route Name, Exit Number.

The development of the NRN was realized by means of individual meetings and national workshops with interested data 
providers from the federal, provincial, territorial and municipal governments.

In 2005, the NRN edition 2.0 was alternately adopted by members from the Inter-Agency Committee on Geomatics (IACG) and 
the Canadian Council on Geomatics (CCOG). The NRN content largely conforms to the ISO 14825 from ISO/TC 204.

Purpose
-------

The National Road Network (NRN) provides quality geospatial and attributive data (current, accurate, consistent), homogeneous and normalized of the entire Canadian road network.

The NRN data serve as a foundation for several applications. This common geometric base is maintained on a regular basis by closest to the source organizations selected for their specific interests or for their ease in offering adequate, up-to-date representations of road phenomena, in accordance with the GeoBase initiative (www.geobase.ca). This common infrastructure facilitates data integration of NRN data with supplementary data.

Topic category
--------------

Main topics for the product, as defined by the ISO 19115 standard or CAN/CGSB 171.100-2009:

013 – location

018 – transportation

Spatial representation type
---------------------------

Type of spatial representation for the product, as defined by the ISO 19115 standard: 

001 - vector

Spatial resolution
------------------
Spatial resolution denominator of the data: 10 000

.. note::
    The nominal spatial resolution is only a general estimate since the data originate from multiple sources (GPS, 
    existing federal, provincial or municipal data) but is approximately 1:10 000.

Geographic description
----------------------

Authority
^^^^^^^^^

International Organization for Standardization (ISO)

Title
"""""

Standard for codes of geographical regions:

ISO 3166-1:1997 Codes for the representation of names of countries and their subdivisions – Part 1: Country codes

Date
""""

Reference date of the ISO 3166-1 standard: 1997-10-01

Date type code
""""""""""""""

Type of date according to ISO 19115 standard: 002 - publication

Code
^^^^

Code of the geographical region covered by the product according to the ISO 3166-1 standard: CA – Canada

Extent type code
^^^^^^^^^^^^^^^^

Extent type code of the delimitation polygon according to the ISO 19115 standard: 

1 - inclusive (the delimitation polygon is inclusive)

Reference to specification scope
--------------------------------

Global

Data Content and Structure
==========================

Description
-----------

The NRN product is distributed in the form of thirteen provincial or territorial datasets and consists of two linear 
entities (Road Segment and Ferry Connection Segment) and three punctual entities (Junction, Blocked Passage, Toll 
Point) with which is associated a series of descriptive attributes such as, among others: First House Number, Last 
House Number, Street Name Body, Place Name, Functional Road Class, Pavement Status, Number Of Lanes, Structure Type, 
Route Number, Route Name, Exit Number.

Addressing information (address range, street name and place name) linked to Road Segment entities are also distributed in three distinct tables (Address Range, Street and Place Names and Alternate Name Link).

Data modelling schema used
--------------------------

Application schema
^^^^^^^^^^^^^^^^^^

The conceptual model of the NRN data is presented in the document *National Road Network, edition 2.0.1, Conceptual 
Data Model, Segmented View* accessible on the GeoBase portal (www.geobase.ca).

The physical implementation of the NRN product differs from the conceptual model in what concerned the management of 
object metadata and the addition of certain attributes to the entity Road Segment.

For the Object Metadata, the conceptual model foresees two distinct series of metadata attributes describing the 
respective sources used for the creation and the revision of the data. Only the creation and revision dates were 
distinctly specified. When a revision date is indicated and a geometric modification was applied on the object (with 
regard to the previous dataset edition), the series of metadata attributes describes the sources used for revision. 
Otherwise, Object Metadata attributes describe the sources used for creation.

The street name, place name and address range were also added as attributes on the geometry of the entity Road Segment.

The document *National Road Network, edition 2.0.1, Product Distribution Formats* also demonstrates the implementation 
of the conceptual model into the physical model of the NRN data product according to the distribution formats GML, 
KML and Shape (www.geobase.ca).

Feature catalogue
^^^^^^^^^^^^^^^^^

The feature catalogue entitled *National Road Network, edition 2.0.1, Feature Catalogue* can be found on the GeoBase 
portal (www.geobase.ca).

Reference to specification scope
--------------------------------

Global (see 2.1)

Reference System
================

Spatial reference system
------------------------

Spatial data are expressed in latitude (φ) and longitude (λ) geographic coordinates in reference to the North American 
Datum 1983 Canadian Spatial Reference System (NAD83CSRS). The longitude is stored as a negative number to represent a 
position west of the prime meridian (0°). Coordinates measuring unit is the degree expressed as a 7-decimal real value.

Authority
^^^^^^^^^

Title
"""""

Coordinate reference system registry: EPSG Geodetic Parameter Dataset

Date
""""

Reference date: 2007-02-08

Date type code
""""""""""""""
Date type according to ISO 19115 standard:

002 - publication

Responsible party
"""""""""""""""""

OGP – International Organisation of Oil and Gas Producers URL: http://www.epsg.org

Code
^^^^

Coordinate reference system identifier (CRSID): 4617

Code space
^^^^^^^^^^

EPSG – European Petroleum Survey Group

Version
^^^^^^^

6.12

Reference to specification scope
--------------------------------

Global (see 2.1)

Data quality
============

Completeness
------------

NRN product contains a quality geometric and attributive description (current, accurate, consistent), homogeneous and 
standardised of the entire Canadian road network.

NRN road representation corresponds to centerline of all non restricted usage roads (5 meters wide or more, drivable 
and not blocked by an obstacle). Roads isolated from the main road network, alleyways, resource and recreational roads 
and addressing information may not be included in some NRN datasets.

.. note::
    Ferry connection segments are included in the NRN for the purpose of road network completeness.

Commission
^^^^^^^^^^

Evaluation methods used for the detection of data in excess is determined by each data provider.

Omission
^^^^^^^^

Evaluation methods used for the detection of missing data is determined by each data provider.

Logical consistency
-------------------

Conceptual consistency
^^^^^^^^^^^^^^^^^^^^^^

The conceptual model of the NRN data can be found in the document *National Road Network, edition 2.0.1, Conceptual 
Data Model, Segmented View* accessible on the GeoBase portal (www.geobase.ca).

The physical implementation of the NRN product differs from the conceptual model in what concerned the management of 
object metadata and the addition of certain attributes to the entity Road Segment.

For the Object Metadata, the conceptual model foresees two distinct series of metadata attributes describing the 
respective sources used for the creation and the revision of the data. Only the creation and revision dates were 
distinctly specified. When a revision date is indicated and a geometric modification was applied on the object (with 
regard to the previous dataset edition), the series of metadata attributes describes the sources used for revision. 
Otherwise, Object Metadata attributes describe the sources used for creation.

The street name, place name and address range were also added as attributes on the geometry of the entity Road Segment.

Domain consistency
^^^^^^^^^^^^^^^^^^

The attributive values are validated by means of an XML schema containing the definition of the authorized domain 
values defined in the feature catalogue.

Authorized combinations of attribute values are validated by means of in-house software.

Format consistency
^^^^^^^^^^^^^^^^^^

The NRN data formats conform to the distribution formats described in the document *National Road Network, edition 2.0.1, 
Product Distribution Formats* accessible on the GeoBase portal (www.geobase.ca).

Topological consistency
^^^^^^^^^^^^^^^^^^^^^^^

The spatial relations of the entities of NRN datasets are systematically validated by means of in-house software.

The validation performed consists in detecting and correcting within reasonable measures: duplication of entities, 
connection and valency between the linear and punctual entities, assignment of identifiers (NID), geometrical 
incoherence ("spikes") and network continuity of route number, route name, street name and address ranges.

Positional accuracy
-------------------

Absolute external positional accuracy
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The geometric accuracy of objects is given as the difference between objects position in the dataset and their real 
ground positions measured in reference to the geodetic network. The accuracy may vary from one object to another. It 
is thus provided in attribute with each feature occurrence and is expressed according to the Circular Map Accuracy 
Standard (CMAS).

Standard Circular Error:

.. math::
    \sigma_c = 0.7071 (\sigma_x^2 + \sigma_y^2)

    \sigma_x : standard deviation in the X-axis

    \sigma_y : standard deviation in the Y-axis

CircularMapAccuracyStandard: 

.. math:: CMAS=2.1460 \sigma_c

The planimetric accuracy aimed for the product is 10 meters or better. Under the data maintenance phase, no systematic 
validation of geometric and attributive accuracies is performed on all NRN datasets.

Data accuracy is evaluated according to the methods used to control acquisition sources (GPS, imagery, photogrammetry, 
etc.) and the positioning errors inherent to data extraction. The method for evaluating data accuracy is determined by 
the data provider.

Relative internal positional accuracy
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Unknown

Temporal accuracy
-----------------

Accuracy of a time measurement
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Not applicable

Temporal consistency
^^^^^^^^^^^^^^^^^^^^

Not applicable

Temporal validity
^^^^^^^^^^^^^^^^^

Not applicable

Thematic accuracy
-----------------

Thematic classification correctness
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Unknown

Non quantitative attribute accuracy
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The method used for evaluating the accuracy of the non quantitative attribute values with respect to reality is 
determined by each data provider.

Quantitative attribute accuracy
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The method used for evaluating the accuracy of the quantitative attribute values with respect to reality is 
determined by each data provider.

Reference to specification scope
--------------------------------

Global (see 2.1)

Data Capture
============

Description
-----------

The method used for data acquisition is determined by each data provider. The selected method must allow for the 
acquisition of quality geospatial data (current, accurate, consistent) for the entire dataset. Many acquisition sources 
are used: GPS, orthoimages, orthophotos, photogrammetry.

Acquisition technique used by the provider is indicated in the object metadata associated with each entity occurrences.

Reference to specification scope
--------------------------------

Global (see 2.1)

Data Maintenance
================

Description
-----------

Maintenance of the NRN data is done within the framework of intergovernmental partnership agreements (federal, 
provincial, territorial and municipal) by interested closest to source governmental bodies capable of offering 
adequate and current representations of the road phenomena. The minimal frequency of maintenance updates is of at 
least once a year.

In order to help NRN data users in their management of the various update releases, updates are packaged and 
distributed by change effects (addition, retirement, modification, confirmation). By proceeding in this fashion, 
identification rules as well as methods for classifying the modifications are established.

Identification rules on how to unequivocally identify entity occurrences are defined in the document entitled *National 
Vector Data – Identification Rules* available on the GeoBase portal (www.geobase.ca).

The methods for classifying updates by change effects (addition, retirement, modification and confirmation) are 
defined in the document *National Vector data – Change Management* available on the GeoBase portal (www.geobase.ca).

Reference to specification scope
--------------------------------

Global

Data Product Delivery
=====================

Output file formats available for the product are: GML (*Geography Markup Language*), KML (*Keyhole Markup Language*) 
and SHAPE (ESRI |trade|).

.. note:: 
    Data files distributed in KML format only contain the entity Road Segment and a subset of attributes.

Delivery format information on GML
----------------------------------

Format name
^^^^^^^^^^^

GML – Geography Markup Language

Version
^^^^^^^

2.1.2

Specification
^^^^^^^^^^^^^

Geography Markup Language – GML – 2.1.2, OpenGIS® Implementation Specifications, 17 September 2002, OGC Document 
Number 02-069 (http://portal.opengeospatial.org/files/?artifact_id=11339)

Language
^^^^^^^^

Languages used in the dataset according to ISO 639-2 standard:

eng-English

fra-French

Delivery format information on KML
----------------------------------

Format name
^^^^^^^^^^^

KML – Keyhole Markup Language

Version
^^^^^^^

2.1

Specification
^^^^^^^^^^^^^

Open Geospatial Consortium Inc., KML 2.1 Reference – An OGC Best Practice, Version 0.0.9, 2007-05- 02, Reference number 
of this OGC® project document: OGC 07-039r1 (http://www.opengeospatial.org/standards/bp)

KML specifications available on GoogleTM Earth web site (http://www.keyhole.com/kml/kml_doc.html).

Language
^^^^^^^^

Languages used in the dataset according to ISO 639-2 standard: 

eng-English

fra-French

Delivery format information on SHAPE
------------------------------------

Format name
^^^^^^^^^^^

Shape – ESRI |trade|

Version
^^^^^^^

01

Specification
^^^^^^^^^^^^^

ESRI Shapefile Technical Description, an ESRI White Paper, July 1998 
(http://www.esri.com/library/whitepapers/pdfs/shapefile.pdf)

Language
^^^^^^^^

Languages used in the dataset according to ISO 639-2 standard: 

eng-English

fra-French

Delivery format information on GPKG
-----------------------------------

Format name
^^^^^^^^^^^

GPKG – OGC Geopackage

Version
^^^^^^^
1.0.1

Specification
^^^^^^^^^^^^^

OGC GeoPackage (https://www.geopackage.org/spec101/index.html)

Language
^^^^^^^^

Languages used in the dataset according to ISO 639-2 standard: 

eng-English

fra-French

Delivery medium information
---------------------------

Units of delivery
^^^^^^^^^^^^^^^^^

Canadian Province/Territory

Medium name
^^^^^^^^^^^

Data are available on the GeoBase portal (www.geobase.ca). 

Other delivery information
^^^^^^^^^^^^^^^^^^^^^^^^^^

The name of the files, entities and attributes are described in the document *National Road Network, edition 2.0.1, 
Product Distribution Formats* (www.geobase.ca).

Data are subject to the GeoBase Unrestricted Use Licence Agreement (www.geobase.ca). 

Reference to specification scope
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Global

Metadata
========

Not applicable

Reference to specification scope
--------------------------------

Global