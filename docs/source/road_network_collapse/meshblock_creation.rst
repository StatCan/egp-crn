******************
Meshblock Creation
******************

.. contents:: Contents:
   :depth: 5

Overview
========

A new meshblock (geographical polygon units) needs to be developed based on the NRN road geometry and NGD BOs (boundary
only arcs). The lowest level of polygons created from these geometries will be the foundational layer of the entire
statistical geographic unit hierarchy.

Resources
---------

:CLI Tool: ``egp/src/meshblock/validate_meshblock.py``
:Output: - ``egp/data/interim/validations.log``
         - Basic metrics output to console.
:Editing Environment: ``egp/data/interim/egp_editing_meshblock.qgz``

Log File
--------

The output log will contain a series of standardized logs for each validation output by the script. Each logged
validation will have the same content structure.

**Generic structure:** ::

    <timestamp> - WARNING: E<error code>

    Values:
    <identifier>
    ...

    Query: "<identifier_field>" in ('<identifier>', ...)

**Specific structure:** ::

    2022-01-04 16:00:51 - WARNING: E201

    Values:
    76d283b46076400c900ed84c02ab605f
    c9ac2f60a0814eec9ff56bf95ad79804

    Query: "segment_id" in ('76d283b46076400c900ed84c02ab605f', 'c9ac2f60a0814eec9ff56bf95ad79804')

**Components:**

:Values: A list containing the identifier field value of each record flagged by the validation for the target dataset.
         The actual identifier field may vary depending on the target dataset.
:Query: A QGIS expression to query all records flagged by the validation for the target dataset. This will contain the
        same values as ``Values``.

Editing Process
---------------

.. figure:: /source/_static/meshblock_creation/editing_process_meshblock_creation.png
    :alt: Editing process overview.

    Figure 1: Editing process overview.

QGIS Project - Explanation of Layers
------------------------------------

.. figure:: /source/_static/meshblock_creation/qgis_project_layers.png
    :alt: QGIS table of contents.

    Figure 2: QGIS table of contents.

Explanation of Layers
^^^^^^^^^^^^^^^^^^^^^

:``nrn_bo``: Primary editing layer representing NRN roads and ferries, NGD BOs, and added NGD roads.
:``ngd_road``: NGD roads (non-BOs) to be used for identifying roads which are missing from the NRN and are required for
               BO integration.
:``nrn_bo``: Copy of ``nrn_bo`` intended to be used with a query / filter from validations.log. Helps to clearly see
             features which need to be targeted without having to individually query each one.
:``CanVec Hydro``: Reference layer to help identify features which may be aligned to CanVec Hydro.
:``Esri Satellite``: Reference layer for recent imagery context.

Validations
===========

1. Connectivity
---------------

.. _Validation 100:

Validation 100
^^^^^^^^^^^^^^

**Description:** All BOs must have nodal connections to other arcs (non-logged - used to classify unintegrated BOs and
feed into other 100-series validations).

BO Integration
""""""""""""""

Every BO (boundary-only) arc must be integrated into the NRN dataset, whether it be the actual arc itself or just the
assignment of the identifier to a corresponding NRN arc. Some exceptions exist such as when the BO is truly not
required and is not ``untouchable`` (see :ref:`Validation 102`).

The NRN is considered the ``base`` geometry for the EGP. Therefore, when deciding which arc to modify (NGD or NRN),
modify the NGD data.

Make use of the WMS resources available within your ``.qgz`` file to avoid incorrectly touching BOs.

.. admonition:: Note

    This is a non-logged validation and is purely used to classify all unintegrated BOs and feed into the remaining
    100-series validations.

Scenario: Endpoint Snapping
"""""""""""""""""""""""""""

...

Scenario: Non-Endpoint Snapping
"""""""""""""""""""""""""""""""

...

Scenario: Crossing Arcs
"""""""""""""""""""""""

...

Scenario: Overlapping Arcs
""""""""""""""""""""""""""

...

Scenario: BO Not Required
"""""""""""""""""""""""""

...

Scenario: Bo-to-BO Connection
"""""""""""""""""""""""""""""

...

Scenario: BO-to-Non-BO Connection
"""""""""""""""""""""""""""""""""

...

Scenario: Ferries
"""""""""""""""""

...

Scenario: NatProvTer
""""""""""""""""""""

...

Scenario: CSD Boundary
""""""""""""""""""""""

...

Scenario: No Proper BO Connection
"""""""""""""""""""""""""""""""""

...

Scenario: CanVec Alignment
""""""""""""""""""""""""""

...

Scenario: Unclear Connections
"""""""""""""""""""""""""""""

...

Scenario: Criss-Crossing BOs
""""""""""""""""""""""""""""

...

Validation 101
^^^^^^^^^^^^^^

.. figure:: /source/_static/meshblock_creation/validation_101.png
    :alt: Validation 101 example.

    Figure ?: Validation 101 example.

| **Description:** Unintegrated BO node is <= 5 meters from an NRN road (entire arc).
| **Actions:**

1. Extend / modify the BO to connect with the appropriate NRN arc(s).

.. admonition:: Warning

    Some instances of this validation may be represented by truly disconnected BOs and roads, such as BOs which
    traverse rivers or shorelines. However, those instances should be easily identifiable since the BO would not be a
    dangling arc.

.. admonition:: Note

    Enable imagery WMS layer in QGIS table of contents to assist in determining feature connectivity.

.. _Validation 102:

Validation 102
^^^^^^^^^^^^^^

| **Description:** Untouchable BO identifier is missing.
| **Actions:**

1. Assign the missing BO identifier (``ngd_uid``) to the appropriate arc(s).

.. admonition:: Definition

    Untouchable BOs: A subset of BOs which must exist in the dataset for other EGP projects. These BO geometries can be
    modified and even deleted (if replaced by an NRN road), but the identifier (``ngd_uid``) must still exist in the
    dataset.

2. Meshblock
------------

Validation 201
^^^^^^^^^^^^^^

.. figure:: /source/_static/meshblock_creation/validation_201.png
    :alt: Validation 201 example.

    Figure ?: Validation 201 example.

| **Description:** All non-deadend arcs (excluding ferries) must form a meshblock polygon.
| **Actions:**

1. Use the integration scenarios defined in :ref:`Validation 100` to correctly connect the arc to the NRN network.

.. admonition:: Note

    In this example, ``ngd_uid=4`` is flagged for not forming a meshblock polygon.

Validation 202
^^^^^^^^^^^^^^

.. figure:: /source/_static/meshblock_creation/validation_202.png
    :alt: Validation 202 example.

    Figure ?: Validation 202 example.

| **Description:** All deadend arcs (excluding ferries) must be completely within 1 meshblock polygon.
| **Actions:**

1. Use the integration scenarios defined in :ref:`Validation 100` to correctly connect the arc and BOs.

.. admonition:: Note

    In this example, ``segment_id=1`` is flagged for not being completely within a single meshblock polygon.
    ``segment_id=0`` is fine.
