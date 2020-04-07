*********************************************
Product Distribution Formats - Segmented View
*********************************************

.. include :: <isonum.txt>

.. contents::
   :depth: 3

Overview
========

The following entities are part of the National Road Network (NRN) Segmented view: 
*Address Range*, *Alternate Name Link*, *Blocked Passage*, *Ferry Connection Segment*, 
*Junction*, *Crossing*, *Road Segment*, *Street and Place Names*, and *Toll Point*.

The product is available in the following output file formats: GML (Geography Markup 
Language), KML (Keyhole Markup Language) SHAPE (ESRI |trade|).

.. note:: 
    Data files distributed in KML format only contain the entity *Road Segment* and a subset 
    of attributes.

Product Identification
======================

:Name: National Road Network
:Version: 2.1
:Date: 2012-03-31
:Standard: National Road Network: Data Product Specifications, Edition 2.1, 2012-03-31
:Feature catalogue: National Road Network: Feature Catalogue, Edition 2.1, 2012-03-31

Distribution Formats Identification
===================================

GML – Geography Markup Language
-------------------------------

:Name: GML – Geography Markup Language
:Version: 2.1.2
:Date: 2002-09-17
:Specifications: Geography Markup Language – GML – 2.1.2, OpenGIS®Implementation Specifications, 
    OGC Document Number 02-069 (http://portal.opengeospatial.org/files/?artifact_id=11339)
 
KML – Keyhole Markup Language
-----------------------------

:Name: KML – Keyhole Markup Language
:Version: 2.2
:Date: 2008-04-14
:Specifications: Open Geospatial Consortium Inc., OGC® KML, Version 2.2.0, 2008-04-14, 
    Reference number of this OGC® project document: OGC 07-147r2 
    (http://portal.opengeospatial.org/files/?artifact_id=27810)

Shape – ESRI |trade|
--------------------

:Name: Shape
:Version: 01
:Date: July 1998
:Specifications: ESRI Shapefile Technical Description, an ESRI White Paper, July 1998 
    (http://www.esri.com/library/whitepapers/pdfs/shapefile.pdf)
 
GPKG – OGC Geopackage
---------------------

:Name: GeoPackage
:Version: 1.0.1
:Date: January 2019
:Specifications: https://www.geopackage.org/spec101/index.html

Distribution Files Identification
=================================

GML File Names
--------------

NRN entities distributed in GML format are grouped into separate dataset files. One file 
contains the geometrical entities and associated basic attributes, another file contains 
the addressing attributes tables, and finally up to four change management files (one for 
each type of content) are available. The name of a GML file is structured accordingly::

    NRN_<IDENTIFIER>_<edition>_<version>_<CONTENT>[_<MODIFICATION>].gml

* NRN = Abbreviated title of the product.
* <IDENTIFIER> = Code of a province or a territory corresponding to the dataset location. 
  Possible codes are: AB, BC, MB, ON, NB, NL, NS, NT, NU, PE, QC, SK, YT.
* <edition> = Dataset edition number.
* <version> = Dataset version number.
* <CONTENT> = Dataset content identifier. Possible values are: GEOM (Geometrical entities and 
  basic attributes), ADDR (Address attributes tables).
* [<MODIFICATION>] = [] = Optional. Type of modification applied to the dataset entities and 
  attributes in comparison to previous edition. Possible values are identified in section 4.5.2.
* .gml = File name extension.

Examples:

* ``NRN_AB_4_0_GEOM.gml`` (Geometrical entities and basic attributes of the dataset of Alberta, 
  edition 4, version 0).
* ``NRN_AB_4_0_ADDR.gml`` (Tables of addressing attributes of the dataset of Alberta, edition 4, 
  version 0).
* ``NRN_AB_4_0_GEOM_ADDED.gml`` (Geometrical entities and/or basic attributes added in the 
  dataset of Alberta, edition 4, version 0).
* ``NRN_AB_4_0_ADDR_ADDED.gml`` (Tables of the addressing attributes added in the dataset of 
  Alberta, edition 4, version 0).

GeoBase
^^^^^^^

An XML schema (XSD file) is also provided along with a GML data file. This file defines, 
in a structured manner, the type of content, the syntax and the semantic of GML documents. 
The name of this file is ``NRN_<IDENTIFIER>_<edition>_<version>_<CONTENT>[_<MODIFICATION>].xsd`` 
and a reference is recorded within the GML file.

KML file name
-------------

The entity Road Segment (and a subset of attributes) is the only entity part of the product 
that is distributed in KML format. The name of the KML file is structured accordingly::

    nrn_rrn_<identifier>_kml_en.kmz

* nrn_rrn = Abbreviated English and French product title.
* <identifier> = Code of a province or a territory corresponding to the dataset location. Possible codes are: ab, bc, mb, on, nb, nl, ns, nt, nu, pe, qc, sk, yt.
* kml Dataset distribution format.
* en ISO code of the dataset distribution language.
* .kmz = File name extension.

Example:

* ``nrn_rrn_ab_kml_en.kmz`` (Road Segment for dataset of Alberta).

Shape file names
----------------

The entities of the product distributed in Shape format are divided according to their 
geometrical representation. The name of the Shape files is structured accordingly::

    NRN_<IDENTIFIER>_<edition>_<version>_<ENTITY>[_<MODIFICATION>].shp

* NRN = Abbreviated product title.
* <IDENTIFIER> = Code of a province or a territory corresponding to the dataset location. 
  Possible codes are: AB, BC, MB, ON, NB, NL, NS, NT, NU, PE, QC, SK, YT.
* <edition> = Dataset edition number.
* <version> = Dataset version number.
* <ENTITY> = Abbreviated entity name as defined in section 4.5.1.
* [<MODIFICATION>] = [] = Optional. Type of modification applied to the dataset entities and 
  attributes in comparison to previous edition. Possible values are identified in section 4.5.2.
* .shp = Extension of the main geometry file name.

There are also five other files associated with the main geometry file of an entity in Shape 
format:

* an attribute file (.dbf for dBASE® file);
* a projection file (.prj) which includes information about the reference system and the 
  parameters of the cartographic projection;
* an index file (.shx) containing the offset (relative position) for each record of the main 
  geometry file;
* two spatial index files for the geometrical data (.sbn, .sbx).

Examples:

* ``NRN_AB_4_0_ROADSEG.shp`` (Entity Road segment for dataset of Alberta, edition 4, version 0);
* ``NRN_AB_4_0_ROADSEG_ADDED.shp`` Road segment in dataset of Alberta, GPKG file names
  (Geometrical entities and/or basic attributes added to edition 4, version 0).

GeoPackage File Names
---------------------

The entities of the product distributed in GeoPackage format are distributed as a single file, 
with the entities divided into layers according to their geometrical representation. The name 
of the GeoPackage file is structured accordingly::

    NRN_<IDENTIFIER>_<edition>_<version>_<ENTITY>[_<MODIFICATION>].gpkg

* NRN = Abbreviated product title.
* <IDENTIFIER> = Code of a province or a territory corresponding to the dataset location. 
  Possible codes are: AB, BC, MB, ON, NB, NL, NS, NT, NU, PE, QC, SK, YT.
* <edition> = Dataset edition number.
* <version> = Dataset version number.
* <ENTITY> = Abbreviated entity name as defined in section 4.5.1.
* [<MODIFICATION>] = [] = Optional. Type of modification applied to the dataset entities and 
  attributes in comparison to previous edition. Possible values are identified in section 4.5.2.
* .shp = Extension of the main geometry file name.

Examples:

* ``NRN_AB_4_0_ROADSEG.gpkg`` (All entities for dataset of Alberta, edition 4, version 0);

Metadata File
-------------

There are four metadata files that are distributed with each dataset of an NRN product. Two 
files are provided in FGDC/XML format (in French and in English) and two others according 
to FGDC/HTML format. The name of the metadata file is structured accordingly::

    nrn_rrn_<identifier>_<edition>_<version>_fgdc_<code language>.<format>

* nrn_rrn = Abbreviated English and French product title.
* <identifier> = Code of a province or a territory corresponding to the dataset location. 
  Possible codes are: ab, bc, mb, on, nb, nl, ns, nt, nu, pe, qc, sk, yt.
* <edition> = Dataset edition number.
* <version> = Dataset version number.
* fgdc = Metadata file format according to CSDGM standard of the Federal Geographic Data 
  Committee (FGDC).
* <code language> = Metadata ISO code language written in lowercase: fr (French), en (English).
* <format> = File name extension (xml or html).

Examples:

* ``nrn_rrn_ab_4_0_fgdc_en.xml`` (English metadata file for dataset of Alberta, edition 4, 
  version 0 in FGDC/XML format)
* ``nrn_rrn_ab_4_0_fgdc_fr.xml`` (French metadata file for dataset of Alberta, edition 4, 
  version 0 in FGDC/HTML format)

List of distribution file names
-------------------------------

The NRN product is comprised of two types of datasets: a file that contains up to date 
(actualized) data (e.g. that has been updated) and a file containing the modifications 
(differences) applied to the previous edition of the dataset.

Dataset
^^^^^^^

The name of a file in GML format is NRN_<IDENTIFER>_<edition>_<version>_<CONTENT>. The 
name of a file in Shape format is NRN_<IDENTIFER>_<edition>_<version>_<ENTITY>. The 
extension of the file name corresponds to the distribution format.

+--------------------------+------------------------+-----------------+----------+
| Feature catalogue        | GML/KML* Entity        | Shape File name | Type     |
| Entity name              | name                   | (``<entity>``)  |          |
+==========================+========================+=================+==========+
| Address Range            | AddressRange           | ADDRANGE        | Table ** |
+--------------------------+------------------------+-----------------+----------+
| Alternate Name Link      | AlternateNameLink      | ALTNAMLINK      | Table ** |
+--------------------------+------------------------+-----------------+----------+
| Blocked Passage          | BlockedPassage         | BLKPASSAGE      | Point    |
+--------------------------+------------------------+-----------------+----------+
| Ferry Connection Segment | FerryConnectionSegment | FERRYSEG        | Line     |
+--------------------------+------------------------+-----------------+----------+
| Junction                 | Junction               | JUNCTION        | Point    |
+--------------------------+------------------------+-----------------+----------+
| Road Segment             | RoadSegment *          | ROADSEG         | Line     |
+--------------------------+------------------------+-----------------+----------+
| Street and Place Names   | StreetPlaceNames       | STRPLANAME      | Table ** |
+--------------------------+------------------------+-----------------+----------+
| Toll Point               | TollPoint              | TOLLPOINT       | Point    |
+--------------------------+------------------------+-----------------+----------+

\* KML content (simplified version of the dataset)

\** Attributes file (.dbf) in Shape format and entities without geometry in GML format.

Change Management Files
^^^^^^^^^^^^^^^^^^^^^^^

Change management consists in identifying the effects of an addition, confirmation, 
retirement and modification of the objects (geometry and/or attribute) between two 
consecutive dataset editions. A data file is produced for each effect type. The name of 
the file in GML format is ``NRN_<IDENTIFIER>_<edition>_<version>_<CONTENT>_<MODIFICATION>`` 
and in Shape format is ``NRN_<IDENTIFIER>_<edition>_<version>_<ENTITY>_<MODIFICATION>``. The 
extension of the file name corresponds to the distribution format.

+-------------------+---------------------------+-----------------------------+
| Change management | GML File name             | Shape File name             |
| Effect name       | (<MODIFICATION>)          | (<MODIFICATION>)            |
+===================+===========================+=============================+
| Added             | <GML File Name>_ADDED     | <Shape File Name>_ADDED     |
+-------------------+---------------------------+-----------------------------+
| Confirmed         | <GML File Name>_CONFIRMED | <Shape File Name>_CONFIRMED |
+-------------------+---------------------------+-----------------------------+
| Modified          | <GML File Name>_MODIFIED  | <Shape File Name>_MODIFIED  |
+-------------------+---------------------------+-----------------------------+
| Retired           | <GML File Name>_RETIRED   | <Shape File Name>_RETIRED   |
+-------------------+---------------------------+-----------------------------+

A readme text file named: ``README_<IDENTIFIER>.txt`` that identifies the method used for the 
*follow-up of the geometrical modifications* is provided with the dataset.

Attributes Identification
=========================

The attributes common to all entities of the NRN product are listed in the first table. The 
attributes specific to each entity are presented in the following subsection.

The data type for all distribution formats is either: C(c) for character or N(n,d) for 
number (c = number of characters, n = total number of digits, d = number of digits in 
decimal).

Attributes Common to All Entities Except Alternate Name Link
------------------------------------------------------------

+------------------------+----------------------+-----------------+-----------+
| Feature Catalogue      | GML Attribute        | Shape Attribute | Shape     |
| Attribute Name         | Name                 | Name            | Data Type |
+========================+======================+=================+===========+
| Acquisition Technique  | acquisitionTechnique | ACQTECH         | C(23)     |
+------------------------+----------------------+-----------------+-----------+
| Coverage               | metadataCoverage     | METACOVER       | C(8)      |
+------------------------+----------------------+-----------------+-----------+
| Creation Date          | creationDate         | CREDATE         | C(8)      |
+------------------------+----------------------+-----------------+-----------+
| Dataset Name           | datasetName          | DATASETNAME     | C(25)     |
+------------------------+----------------------+-----------------+-----------+
| Planimetric Accuracy   | planimetricAccuracy  | ACCURACY        | N(4,0)    |
+------------------------+----------------------+-----------------+-----------+
| Provider               | provider             | PROVIDER        | C(24)     |
+------------------------+----------------------+-----------------+-----------+
| Revision Date          | revisionDate         | REVDATE         | C(8)      |
+------------------------+----------------------+-----------------+-----------+
| Standard Version       | standardVersion      | SPECVERS        | C(10)     |
+------------------------+----------------------+-----------------+-----------+

Attributes Specific to Entities
-------------------------------

Address Range
^^^^^^^^^^^^^

+-----------------------------------------+-------------------------------+-----------------+-----------+
| Feature Catalogue                       | GML Attribute                 | Shape Attribute | Shape     |
| Attribute Name                          | Name                          | Name            | Data Type |
+=========================================+===============================+=================+===========+
| Alternate Street Name NID (left, right) | left_AlternateStreetNameNid   | L_ALTNANID      | C(32)     |
+                                         +-------------------------------+-----------------+-----------+
|                                         | right_AlternateStreetNameNid  | R_ALTNANID      | C(32)     |
+-----------------------------------------+-------------------------------+-----------------+-----------+
| Digitizing Direction Flag (left, right) | left_DigitizingDirectionFlag  | L_DIGDIRFG      | C(18)     |
+                                         +-------------------------------+-----------------+-----------+
|                                         | right_DigitizingDirectionFlag | R_DIGDIRFG      | C(18)     |
+-----------------------------------------+-------------------------------+-----------------+-----------+
| First House Number (left, right)        | left_FirstHouseNumber         | L_HNUMF         | N(9,0)    |
+                                         +-------------------------------+-----------------+-----------+
|                                         | right_FirstHouseNumber        | R_HNUMF         | N(9,0)    |
+-----------------------------------------+-------------------------------+-----------------+-----------+
| First House Number Suffix (left, right) | left_FirstHouseNumberSuffix   | L_HNUMSUFF      | C(10)     |
+                                         +-------------------------------+-----------------+-----------+
|                                         | right_FirstHouseNumberSuffix  | R_HNUMSUFF      | C(10)     |
+-----------------------------------------+-------------------------------+-----------------+-----------+
| First House Number Type (left, right)   | left_FirstHouseNumberType     | L_HNUMTYPE      | C(16)     |
+                                         +-------------------------------+-----------------+-----------+
|                                         | right_FirstHouseNumberType    | R_HNUMTYPE      | C(16)     |
+-----------------------------------------+-------------------------------+-----------------+-----------+
| House Number Structure (left, right)    | left_HouseNumberStructure     | L_HNUMSTR       | C(9)      |
+                                         +-------------------------------+-----------------+-----------+
|                                         | right_HouseNumberStructure    | R_HNUMSTR       | C(9)      |
+-----------------------------------------+-------------------------------+-----------------+-----------+
| Last House Number (left, right)         | left_LastHouseNumber          | L_HNUML         | N(9,0)    |
+                                         +-------------------------------+-----------------+-----------+
|                                         | right_LastHouseNumber         | R_HNUML         | N(9,0)    |
+-----------------------------------------+-------------------------------+-----------------+-----------+
| Last House Number Suffix (left, right)  | left_LastHouseNumberSuffix    | L_HNUMSUFL      | C(10)     |
+                                         +-------------------------------+-----------------+-----------+
|                                         | right_LastHouseNumberSuffix   | R_HNUMSUFL      | C(10)     |
+-----------------------------------------+-------------------------------+-----------------+-----------+
| Last House Number Type (left, right)    | left_LastHouseNumberType      | L_HNUMTYPL      | C(16)     |
+                                         +-------------------------------+-----------------+-----------+
|                                         | right_LastHouseNumberType     | R_HNUMTYPL      | C(16)     |
+-----------------------------------------+-------------------------------+-----------------+-----------+
| NID                                     | nid                           | NID             | C(32)     |
+-----------------------------------------+-------------------------------+-----------------+-----------+
| Official Street Name NID (left, right)  | left_OfficialStreetNameNid    | L_HNUMTYPL      | C(16)     |
+                                         +-------------------------------+-----------------+-----------+
|                                         | right_OfficialStreetNameNid   | R_HNUMTYPL      | C(16)     |
+-----------------------------------------+-------------------------------+-----------------+-----------+
| Reference System Indicator (left, right)| left_ReferenceSystemIndicator | L_HNUMTYPL      | C(16)     |
+                                         +-------------------------------+-----------------+-----------+
|                                         | rght_ReferenceSystemIndicator | R_HNUMTYPL      | C(16)     |
+-----------------------------------------+-------------------------------+-----------------+-----------+

Alternate Name Link
^^^^^^^^^^^^^^^^^^^

+------------------------+----------------------+-----------------+-----------+
| Feature Catalogue      | GML Attribute        | Shape Attribute | Shape     |
| Attribute Name         | Name                 | Name            | Data Type |
+========================+======================+=================+===========+
| Creation Date          | creationDate         | CREDATE         | C(8)      |
+------------------------+----------------------+-----------------+-----------+
| Dataset Name           | datasetName          | DATASETNAM      | C(100)    |
+------------------------+----------------------+-----------------+-----------+
| NID                    | nid                  | NID             | C(32)     |
+------------------------+----------------------+-----------------+-----------+
| Revision Date          | revisionDate         | REVDATE         | C(8)      |
+------------------------+----------------------+-----------------+-----------+
| Standard Version       | standardVersion      | SPECVERS        | C(10)     |
+------------------------+----------------------+-----------------+-----------+
| Street Name NID        | streetNameNid        | STRNAMENID      | C(32)     |
+------------------------+----------------------+-----------------+-----------+

Blocked Passage
^^^^^^^^^^^^^^^

+------------------------+----------------------+-----------------+-----------+
| Feature Catalogue      | GML Attribute        | Shape Attribute | Shape     |
| Attribute Name         | Name                 | Name            | Data Type |
+========================+======================+=================+===========+
| Blocked Passage Type   | blockedPassageType   | BLKPASSTY       | C(17)     |
+------------------------+----------------------+-----------------+-----------+
| NID                    | nid                  | NID             | C(32)     |
+------------------------+----------------------+-----------------+-----------+
| Road Element NID       | roadElementNid       | ROADNID         | C(32)     |
+------------------------+----------------------+-----------------+-----------+

Ferry Connection Segment
^^^^^^^^^^^^^^^^^^^^^^^^

+----------------------------------+----------------------+-----------------+-----------+
| Feature Catalogue                | GML Attribute        | Shape Attribute | Shape     |
| Attribute Name                   | Name                 | Name            | Data Type |
+==================================+======================+=================+===========+
| Closing Period                   | closingPeriod        | CLOSING         | C(7)      |
+----------------------------------+----------------------+-----------------+-----------+
| Ferry Segment ID                 | ferrySegmentId       | FERRYSEGID      | N(9,0)    |
+----------------------------------+----------------------+-----------------+-----------+
| Functional Road Class            | functionlaRoadClass  | ROADCLASS       | C(21)     |
+----------------------------------+----------------------+-----------------+-----------+
| NID                              | nid                  | NID             | C(32)     |
+----------------------------------+----------------------+-----------------+-----------+
| Route Name English (1, 2, 3, 4)  | routeNameEnglish1    | RTENAME1EN      | C(100)    |
+                                  +----------------------+-----------------+-----------+
|                                  | routeNameEnglish2    | RTENAME2EN      | C(100)    |
+                                  +----------------------+-----------------+-----------+
|                                  | routeNameEnglish3    | RTENAME3EN      | C(100)    |
+                                  +----------------------+-----------------+-----------+
|                                  | routeNameEnglish4    | RTENAME4EN      | C(100)    |
+----------------------------------+----------------------+-----------------+-----------+
| Route Name French (1, 2, 3, 4)   | routeNameFrench1     | RTENAME1FR      | C(100)    |
+                                  +----------------------+-----------------+-----------+
|                                  | routeNameFrench2     | RTENAME2FR      | C(100)    |
+                                  +----------------------+-----------------+-----------+
|                                  | routeNameFrench3     | RTENAME3FR      | C(100)    |
+                                  +----------------------+-----------------+-----------+
|                                  | routeNameFrench4     | RTENAME4FR      | C(100)    |
+----------------------------------+----------------------+-----------------+-----------+
| Route Number (1, 2, 3, 4, 5)     | routeNumber1         | RTNUMBER1       | C(10)     |
+                                  +----------------------+-----------------+-----------+
|                                  | routeNumber2         | RTNUMBER2       | C(10)     |
+                                  +----------------------+-----------------+-----------+
|                                  | routeNumber3         | RTNUMBER3       | C(10)     |
+                                  +----------------------+-----------------+-----------+
|                                  | routeNumber4         | RTNUMBER4       | C(10)     |
+----------------------------------+----------------------+-----------------+-----------+

Junction
^^^^^^^^

+------------------------+----------------------+-----------------+-----------+
| Feature Catalogue      | GML Attribute        | Shape Attribute | Shape     |
| Attribute Name         | Name                 | Name            | Data Type |
+========================+======================+=================+===========+
| Exit Number            | exitNumber           | EXITNBR         | C(10)     |
+------------------------+----------------------+-----------------+-----------+
| Junction Type          | junctionType         | JUNCTYPE        | C(12)     |
+------------------------+----------------------+-----------------+-----------+
| NID                    | nid                  | NID             | C(32)     |
+------------------------+----------------------+-----------------+-----------+

Road Segment
^^^^^^^^^^^^

+-----------------------------------------+----------------------------------+-----------------+-----------+
| Feature Catalogue                       | GML Attribute                    | Shape Attribute | Shape     |
| Attribute Name                          | Name                             | Name            | Data Type |
+=========================================+==================================+=================+===========+
| Address Range Digitizing Direction      | left_AddressDirectionFlag *      | L_ADDDIRFG      | C(18)     |
| Flag (left, right)                      +----------------------------------+-----------------+-----------+
|                                         | right_AddressDirectionFlag *     | R_ADDDIRFG      | C(18)     |
+-----------------------------------------+----------------------------------+-----------------+-----------+
| Address Range NID                       | addressRangeNid                  | ADRANGENID      | C(32)     |
+-----------------------------------------+----------------------------------+-----------------+-----------+
| Exit Number                             | closingPeriod                    | CLOSING         | C(32)     |
+-----------------------------------------+----------------------------------+-----------------+-----------+
| Alternate Street Name NID (left, right) | exitNumber                       | EXITNBR         | C(32)     |
+-----------------------------------------+----------------------------------+-----------------+-----------+
| First House Number (left, right)        | left_FirstHouseNumber            | L_HNUMF         | C(30)     |
|                                         +----------------------------------+-----------------+-----------+
|                                         | right_FirstHouseNumber           | R_HNUMF         | C(30)     |
+-----------------------------------------+----------------------------------+-----------------+-----------+
| Functional Road Class                   | functionalRoadClass              | ROADCLASS       | C(21)     |
+-----------------------------------------+----------------------------------+-----------------+-----------+
| Last House Number (left, right)         | left_LastHouseNumber             | L_HNUML         | C(30)     |
|                                         +----------------------------------+-----------------+-----------+
|                                         | right_LastHouseNumber            | R_HNUML         | C(30)     |
+-----------------------------------------+----------------------------------+-----------------+-----------+
| NID                                     | nid *                            | NID             | C(32)     |
+-----------------------------------------+----------------------------------+-----------------+-----------+
| Number Of Lanes                         | numberLanes                      | NBRLANES        | N(4,0)    |
+-----------------------------------------+----------------------------------+-----------------+-----------+
| Official Place Name (left, right)       | left_OfficialPlaceName *         | L_PLACENAM      | C(100)    |
|                                         +----------------------------------+-----------------+-----------+
|                                         | right_OfficialPlaceName *        | R_PLACENAM      | C(100)    |
+-----------------------------------------+----------------------------------+-----------------+-----------+
| Last House Number (left, right)         | left_OfficialStreetNameConcat *  | L_STNAME_C      | C(100)    |
|                                         +-------------------------------+--------------------+-----------+
|                                         | right_OfficialStreetNameConcat * | R_STNAME_C      | C(100)    |
+-----------------------------------------+----------------------------------+-----------------+-----------+
| Paved Road Surface Type                 | pavedRoadSurfaceType             | PAVSURF         | C(8)      |
+-----------------------------------------+----------------------------------+-----------------+-----------+
| Pavement Status                         | pavementStatus                   | PAVSTATUS       | C(7)      |
+-----------------------------------------+----------------------------------+-----------------+-----------+
| Road Jurisdiction                       | roadJurisdiction                 | ROADJURIS       | C(100)    |
+-----------------------------------------+----------------------------------+-----------------+-----------+
| Road Segment ID                         | roadSegmentId                    | ROADSEGID       | N(9,0)    |
+-----------------------------------------+----------------------------------+-----------------+-----------+
| Route Name English (1, 2, 3, 4)         | routeNameEnglish1                | RTENAME1EN      | C(100)    |
+                                         +----------------------------------+-----------------+-----------+
|                                         | routeNameEnglish2                | RTENAME2EN      | C(100)    |
+                                         +----------------------------------+-----------------+-----------+
|                                         | routeNameEnglish3                | RTENAME3EN      | C(100)    |
+                                         +----------------------------------+-----------------+-----------+
|                                         | routeNameEnglish4                | RTENAME4EN      | C(100)    |
+-----------------------------------------+----------------------------------+-----------------+-----------+
| Route Name French (1, 2, 3, 4)          | routeNameFrench1                 | RTENAME1FR      | C(100)    |
+                                         +----------------------------------+-----------------+-----------+
|                                         | routeNameFrench2                 | RTENAME2FR      | C(100)    |
+                                         +----------------------------------+-----------------+-----------+
|                                         | routeNameFrench3                 | RTENAME3FR      | C(100)    |
+                                         +----------------------------------+-----------------+-----------+
|                                         | routeNameFrench4                 | RTENAME4FR      | C(100)    |
+-----------------------------------------+----------------------------------+-----------------+-----------+
| Route Number (1, 2, 3, 4, 5)            | routeNumber1                     | RTNUMBER1       | C(10)     |
+                                         +----------------------------------+-----------------+-----------+
|                                         | routeNumber2                     | RTNUMBER2       | C(10)     |
+                                         +----------------------------------+-----------------+-----------+
|                                         | routeNumber3                     | RTNUMBER3       | C(10)     |
+                                         +----------------------------------+-----------------+-----------+
|                                         | routeNumber4                     | RTNUMBER4       | C(10)     |
+-----------------------------------------+----------------------------------+-----------------+-----------+
| Speed Restrictions                      | speedRestrictions                | SPEED           | N(4,0)    |
+-----------------------------------------+----------------------------------+-----------------+-----------+
| Structure Name English                  | structureNameEnglish             | STRUNAMEEN      | C(100)    |
+-----------------------------------------+----------------------------------+-----------------+-----------+
| Structure Name French                   | structureNameFrench              | STRUNAMEFR      | C(100)    |
+-----------------------------------------+----------------------------------+-----------------+-----------+
| Structure ID                            | structureId                      | STRUCTID        | C(32)     |
+-----------------------------------------+----------------------------------+-----------------+-----------+
| Structure Type                          | structureType                    | STRUCTTYPE      | C(15)     |
+-----------------------------------------+----------------------------------+-----------------+-----------+
| Traffic Direction                       | trafficDirection                 | TRAFFICDIR      | C(18)     |
+-----------------------------------------+----------------------------------+-----------------+-----------+
| Unpaved Road Surface Type               | unpavedRoadSurfaceType           | UNPAVSURF       | C(7)      |
+-----------------------------------------+----------------------------------+-----------------+-----------+

\* KML content (simplified version of the dataset)


Street and Place Names
^^^^^^^^^^^^^^^^^^^^^^

+------------------------+----------------------+-----------------+-----------+
| Feature Catalogue      | GML Attribute        | Shape Attribute | Shape     |
| Attribute Name         | Name                 | Name            | Data Type |
+========================+======================+=================+===========+
| Directional Prefix     | directionalPrefix    | DIRPREFIX       | C(10)     |
+------------------------+----------------------+-----------------+-----------+
| Directional Suffix     | directionalSuffix    | DIRSUFFIX       | C(10)     |
+------------------------+----------------------+-----------------+-----------+
| Muni Quadrant          | muniQuadrant         | MUNIQUAD        | C(10)     |
+------------------------+----------------------+-----------------+-----------+
| NID                    | nid                  | NID             | C(32)     |
+------------------------+----------------------+-----------------+-----------+
| Place Name             | placeName            | PLACENAME       | C(100)    |
+------------------------+----------------------+-----------------+-----------+
| Place Type             | placeType            | PLACETYPE       | C(100)    |
+------------------------+----------------------+-----------------+-----------+
| Province               | province             | PROVINCE        | C(25)     |
+------------------------+----------------------+-----------------+-----------+
| Street Name Article    | streetNameArticle    | STARTICLE       | C(20)     |
+------------------------+----------------------+-----------------+-----------+
| Street Name Body       | streetNameBody       | NAMEBODY        | C(50)     |
+------------------------+----------------------+-----------------+-----------+
| Street Type Prefix     | streetTypePrefix     | STRTYPRE        | C(30)     |
+------------------------+----------------------+-----------------+-----------+
| Street Type Suffix     | streetTypeSuffix     | STRTYSUF        | C(30)     |
+------------------------+----------------------+-----------------+-----------+

Toll Point
^^^^^^^^^^

+------------------------+----------------------+-----------------+-----------+
| Feature Catalogue      | GML Attribute        | Shape Attribute | Shape     |
| Attribute Name         | Name                 | Name            | Data Type |
+========================+======================+=================+===========+
| NID                    | nid                  | NID             | C(32)     |
+------------------------+----------------------+-----------------+-----------+
| Road Element NID       | roadElementNid       | ROADNID         | C(32)     |
+------------------------+----------------------+-----------------+-----------+
| Toll Point Type        | tollPointType        | TOLLPTTYPE      | C(22)     |
+------------------------+----------------------+-----------------+-----------+