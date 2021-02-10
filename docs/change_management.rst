*****************
Change Management
*****************

.. include:: <isopub.txt>

.. contents::
   :depth: 3

Abbreviations
=============

.. glossary::
    ID
        Identifier

    NID
        National Identifier

    NHN
        National Hydrographic Network

    NRCan
        Natural Resources Canada

    NRN
        National Road Network

    NVD
        National Vector Data

    UUID
        Universal Unique Identifiers

Terms and Definitions
=====================

National Vector Data
    Several layers of vector data, referred to as National Vector Data (NVD), will share the
    same specification. The National Road Network (NRN) and National Hydrographic Network (NHN)
    are examples of NVD.

Overview
========

The objective is to update the NVD data on a regular basis as soon as mechanisms have been
established between NVD’s partners. One of the mechanisms is establishing change management
principles. Two basic concepts are needed: identification rules and definition / classification
of change.

:doc:`identification_rules` defines the identification mechanism used as precisely as
possible. In terms of change management, the NVD doesn't attempt to track the evolution of
phenomena in the real world (features), but rather of the objects that represent them. In
other words, the NVD does not monitor real changes in the territory; they identify only the
*effects* they have on the data.

Many projects (or the literature) deal with update management and time modelling [#f1]_,
[#f2]_, [#f3]_. The model herein was developed in cooperation with the Centre for Research
in Geomatics (CRG) at Laval University [#f4]_.

It is aimed to monitor the evolution of objects in order to identify any changes that may
have occurred between two observations, whether successive or not. The discrepancies observed
between two observations are referred to as the differential [#f5]_. Change management also makes
it possible to track updates as well as data corrections. The purpose of update management is
to facilitate synchronization of databases from producing partners and customers based on
current national views (see Figure 1 : Evolution of the database in time).

The update management process must also make it possible to reconstitute the data as it
was on a previous date.

.. figure:: /_static/figures/evolution_of_db_in_time.png
    :alt: Evolution of the database in time

    Figure 1: Evolution of the database in time.

Object Life Cycle
=================

NVD data constitutes the best representation of the real-world phenomena of interest,
unless proven otherwise. The geometric data in the NVD must undergo a minimum of change.
Changes occur when a new information source offers a better representation than the
preceding one.

The effects on NVD data will therefore be established based on the preceding representation.
Data life cycle is therefore limited by two events. The cycle always begins with an
"addition" (assignment of a new NID) and ends with "retirement." Between these two events,
geometric or descriptive modification or confirmation of the preceding state can occur,
while maintaining the same NID. Data with the effects "addition," "geometric or descriptive
modification," and "confirmation" are *active* (or current) features. Features with the
effect "retirement" are *nonactive* (historical) data.

Effect Types
============

Updating makes it possible to establish a parallel between existing data and the new data
from an update. The latter has certain *effects* on the data. The following effects can be
classified as:

Addition (Existence)
--------------------

When a new object has no geometric counterpart in the NVD, a new object is *added*, along
with a new NID.

Retirement (Existence)
----------------------

When an object no longer represents a feature, the object is *retired*. This type of object
is removed from the current data while maintaining its NID.

Modification (Evolution)
------------------------

An object is said to be *modified* if one of its descriptive attributes or its geometric
representation is different. In this case, the initial NID is preserved for the new version
of the object. Two types of modification are possible.

Descriptive Modification
^^^^^^^^^^^^^^^^^^^^^^^^

A descriptive modification occurs when a pair of objects from the same class is geometrically
identical but has different attribute values. For example, the type of surface of a
specific road may have changed from "unpaved" to "paved".

Geometric Modification
^^^^^^^^^^^^^^^^^^^^^^

A geometric modification occurs when a pair of objects from the same class has different
geometries that describe the same phenomena.

There are three types of geometric modification currently defined within the NVD. Each has a
certain level of complexity. In comparing two representations (old and new), it is possible
to define geometric modifications as being:

First Method
""""""""""""

While comparing two objects, if one vertex is different from its previous representation,
the old representation is retired and a new representation added. This method of managing
representation modifications means that *geometric modifications are not followed*.

Second method
"""""""""""""

The second method of managing representation change is based on comparing the old and the
new Junction locations. Two Junctions always bound a Network Linear Element. Any
modification along an element (geometric representation) may occur between its Junctions.
These are treated as a geometric modification while conserving its NID. If, for whatever
reason, one of the old Junctions located at one end of the Network Linear Element has
changed, then this Network Linear Element is retired and a new one added.

Third method
""""""""""""

The third method is based on topological links. If the representation of the Linear
Element Junctions have maintained the same topological links (even if the Junctions
have moved and the Network Linear Element geometry has been modified), then these changes
are treated as a geometric modifications and the Network Linear Element and Junctions
maintain their NIDs.

Confirmation (Evolution)
------------------------

Alongside change, there is confirmation of objects when the geometric or descriptive
attributes have not been modified.

Used Effects
============

Segmented Data
--------------

Feature classes with geometry track change using all *effects* defined in Section 3
(Addition, Descriptive Modification, Retirement and Confirmation).

In the segmented model, the effect must be associated to the complete Network Linear
Element even if this one is broken up into several segments because of a change of
attribute. I.e. the same effect must be applied to all the segments that have the same
value of attribute NID. The segments that describe the same Network Linear Element can
have only one effect and the set of priorities is as follows: Addition, Descriptive
Modification and Confirmation.

The follow-up method for geometric modifications is indicated by the change management
provider.

Example
=======

The following example is designed to illustrate update management for better comprehension.
Figure 2 : Example of an update demonstrates the comparison between the original data and
new data. In terms of geometry, a single Road Element (object 6) was added with respect to
the original data. In terms of description, the type of surface of the Road Element (object 2)
has changed from unpaved to paved.

.. figure:: /_static/figures/example_of_an_update.png
    :alt: Example of an update

    Figure 2: Example of an update.

Table 1: Updating effects shows the geometric effects observed after the update.

+---------+-------------------------------------------------------+--------------+
| Objects | Explanation                                           | Effects      |
+=========+=======================================================+==============+
| 3       | No correspondence with a new object.                  | Retirement   |
+---------+-------------------------------------------------------+--------------+
| 4       | No correspondence with an object in the original      | Addition     |
|         | data; the arrival of object 6 changed the topological |              |
|         | structure of the objects (and therefore the           |              |
|         | geometry).                                            |              |
+---------+-------------------------------------------------------+--------------+
| 5       | No correspondence with an object in the original      | Addition     |
|         | data; the arrival of object 6 changed the topological |              |
|         | structure of the objects (and therefore the           |              |
|         | geometry).                                            |              |
+---------+-------------------------------------------------------+--------------+
| 6       | No correspondence with an object in the original      | Addition     |
|         | data; the feature was not represented.                |              |
+---------+-------------------------------------------------------+--------------+
| e       | No correspondence with an object in the original      | Addition     |
|         | data.                                                 |              |
+---------+-------------------------------------------------------+--------------+
| f       | No correspondence with an object in the original      | Addition     |
|         | data.                                                 |              |
+---------+-------------------------------------------------------+--------------+
| 2       | Attribute value changed.                              | Description  |
|         |                                                       | modification |
+---------+-------------------------------------------------------+--------------+
| 1       | Geometry and attributes did not change.               | Confirmation |
+---------+-------------------------------------------------------+--------------+
| a       | Geometry and attributes did not change.               | Confirmation |
+---------+-------------------------------------------------------+--------------+
| b       | Geometry and attributes did not change.               | Confirmation |
+---------+-------------------------------------------------------+--------------+
| c       | Geometry and attributes did not change.               | Confirmation |
+---------+-------------------------------------------------------+--------------+
| d       | Geometry and attributes did not change.               | Confirmation |
+---------+-------------------------------------------------------+--------------+

Table 1: Updating effects.

References
==========

.. [#f1] Langran, Gail. Time in Geographic Information Systems, Éd.Taylor & Francis,
    1993, 187 p.
.. [#f2] PEUQUET, Donna J. It's About Time: A Conceptual Framework for the
    Representation of Temporal Dynamics in Geographic Information Systems, Annals of the
    Association of American Geographers, vol. 84, No. 3, 1994, p. 441-461.
.. [#f3] Worboys, Michael F. A Unified Model for Spatial and Temporal Information,
    The Computer Journal, Vol 37, No. 1, pp. 26-34
.. [#f4] Pouliot, J, Larrivé, S., and Bédard, Y. Typologie des mises à jour, 2000,
    11 p.
.. [#f5] The differential corresponds to the set of differences observed between two
    landmarks of the territory [#f4]_.