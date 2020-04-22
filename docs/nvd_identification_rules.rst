*******************************************
National Vector Data - Identification Rules
*******************************************

.. contents::
   :depth: 3

Abbreviations
=============

.. glossary::
    GUID
        Globally Unique Identifiers

    ID
        Identifier

    IEEE
        Institute of Electrical and Electronics Engineers NHN National Hydrographic Network

    NID
        National Identifier
    
    NRCan
        Natural Resources Canada
    
    NRN
        National Road Network
    
    NSDI
        National Spatial Data Infrastructure – USA NVD National Vector Data

    UUID
        Universal Unique Identifiers

Terms and Definitons
====================

Network Linear Element
    Several layers of vector data, referred to as National Vector Data (NVD), will share 
    the same specification. The National Road Network (NRN) and National Hydrographic 
    Network (NHN) are examples of NVD.

Overview
========

The objective is to update the NVD product on a regular basis as soon as mechanisms have 
been established between NRN partners. One of the update mechanisms is by establishing 
change management principles. The establishment of change management principles is founded 
on two basic concepts: identification rules and definition / classification of change.

The concept of Identification is based on objects that depict real-world phenomena that 
vary over time, either by their description, by their precision or by the instruments and 
methods involved in their initial acquisition. It is therefore possible that more than one 
representation of the same phenomena may exist. To illustrate this fact, as part of this 
initiative, the goal is to build and maintain a **single** representation of the National 
Road Network. Identifiers play a fundamental role in ensuring long-term distributed data 
management and for implementing update mechanisms of Objects modified at the source and 
already provided to users. The implementation of a standard for the permanent identification 
of a phenomenon and its application must achieve two primary objectives:

* Facilitate the management and distribution of object changes in an incremental manner;
* Facilitate the conflation process, if necessary.

Every occurrence of NVD basic features must be uniquely identified: As an example, each 
geometric object in the NRN: *Road Element*, *Ferry Connection*, and *Junction* that describe 
specific characteristics of the linear network must also be uniquely identified.

Identification Standard
=======================

The Identifiers must be permanently assigned or persistent. To ensure their stability, the 
assigned IDs must be insignificant (inconsequential) in their expression [#f1]_. In other 
words, the IDs must not contain any information relative to the data. Past experience has 
demonstrated that encapsulating information within the ID can cause ID modification 
without any real change ever having occurred in the data.

Several standards have dealt with the Road Network. Most of them point to the importance of 
using Identifiers without ever specifying the manner, format, or method of application. 
GDF [#f2]_, GIS-T (GIS in transportation data standards) [#f3]_ and CEN TC 278 [#f4]_ 
documentation have no specifications related to Identifiers. National Spatial Data 
Infrastructure – USA (NSDI) Framework Transportation Identification Standard was the only 
document that clearly defined and described an Identifier code [#f5]_. However, within the 
ISO TC 211/SC: Geographic Information Standard - Encoding [#f6]_ the UUID definition did 
comply to the fundamental requirements sought after:

    “An application domain defines a universe and an identification scheme called 
    universal unique identifiers (UUIDs). A UUID is assigned to an object when it is 
    created and is stable over the object's entire life span. The UUID of a deleted object 
    cannot be used again. UUIDs are required for long-term distributed data management and 
    for implementing update mechanisms. These identifiers are also called persistent 
    identifiers. A special name server may be used to resolve persistent identifiers. The 
    identifiers are unique within a well-defined limited universe defined by an application 
    domain.”

This ISO definition is thus adopted for the “Identifier”. A UUID generation mechanism is 
presented in the following section.

NVD Identification Standard
===========================

ID uniqueness is one of the fundamental characteristics that must be maintained. Two 
techniques for making IDs unique were studied.

* The first consists of mandating a firm to generate and manage ID ranges depending on data 
  producers.
* The second consists of using a unique ID generation algorithm [#f7]_ that could be used by data 
  producers with no particular management of range and domain.

The *second* method is best suited and was the one retained.

A UUID is an identifier that is unique across both space and time, with respect to the space 
of all UUIDs. UUID generation does not require a registration authority for each single 
identifier. Instead, it requires a unique value over space for each UUID generator. This 
spatially unique value is specified as an IEEE 802 address, which is usually already applied 
to network-connected systems. This 48-bit address can be assigned based on an address block 
obtained through the IEEE registration authority. This UUID specification assumes the 
availability of an IEEE 802 address.

The UUID consists of a 16-byte record and must void of padding between fields. The hexadecimal 
values “a” to “f” must be lower case. The total size is 128 bits. For use as human-readable 
text, a UUID string representation (32 characters) is specified as a sequence of fields. The 
following string is a UUID example:

* 378a3917e824422cb25f268b8295da51

For more information: http://www.opengroup.org/onlinepubs/9629399/apdxa.htm#tagcjh_20

The assignation and persistence rules of the UUID are further explained in the 
:doc:`nvd_change_management` document.

NID Values
==========

The algorithm described in the previous section provides producers the needed flexibility 
while working within a network of partners. The algorithm can be used by all closest to 
source data producers to modify the data and add a new NID when needed. **NIDs should only be 
generated and assigned by authorized organizations**. Specific care must be given to the 
management of NIDs. These NIDs will eventually allow for data synchronization between 
organizations. Data users must ensure that they make **no alterations whatsoever to these** NIDs 
value in order to ensure synchronization. Modifications to NID’s would render them useless 
for data synchronization.

Footnotes
=========

.. [#f1] Bédard Y, Larrivé S et Proulx M-J. “Travaux de modélisation pour la mise en place de 
    la base de données géospatiale“ ISIS, Laval University, March 2000
.. [#f2] ISO Technical Committee 204, Working group 3, “ISO/TR 14825 GDF – Geographic Data 
    Files – Version 4.0,” ISO/TC 204 N629, October 12, 2000
.. [#f3] Dueker, Kenneth J. and Butler, J. Allison, “GIS-T Enterprise Data Model with 
    Suggested Implementation Choices“,Center for Urban Studies School of Urban and Public 
    Affairs Portland State University, October 1, 1997
.. [#f4] http://www.nen.nl/cen278
.. [#f5] National Spatial Data Infrastructure, “NSDI FRAMEWORK TRANSPORTATION IDENTIFICATION 
    STANDARD -- Public Review Draft,” FGDC-STD-999.1-2000, Ground Transportation Subcommittee 
    Federal Geographic Data Committee, December, 2000
.. [#f6] ISO Technical Committee 211, Working Group 4, “Geographic Information – Encoding,” 
    ISO/CD 19118.3, June 15, 2001
.. [#f7] Readers wishing to use a standards-body definition of UUIDs/GUIDs should refer 
    to: ISO/IEC 11578:1996 Information technology -- Open Systems Interconnection -- Remote 
    Procedure Call http://www.iso.org/iso/en/CatalogueDetailPage.CatalogueDetail?CSNUMBER=2229&ICS1=35&ICS2=100&ICS3=70 
    or DCE 1.1: Remote Procedure Call Open Group Technical Standard Document Number C706, 
    August 1997, 737 pages. (Supersedes C309 DCE: Remote Procedure Call 8/94, which was the 
    basis for the ISO specification) http://www.opengroup.org/publications/catalog/c706.htm