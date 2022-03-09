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

QGIS Project
------------

Explanation of Layers
^^^^^^^^^^^^^^^^^^^^^

.. figure:: /source/_static/meshblock_creation/qgis_project_layers.png
    :alt: QGIS table of contents.

    Figure 2: QGIS table of contents.

**Layers:**

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

.. _Meshblock Creation Validation 100:

Validation 100
^^^^^^^^^^^^^^

**Description:** All BOs must have nodal connections to other arcs (non-logged - used to classify unintegrated BOs and
feed into other 100-series validations).

BO Integration Overview
"""""""""""""""""""""""

Every BO (boundary-only) arc must be integrated into the NRN dataset, whether it be the actual arc itself or just the
assignment of the identifier to a corresponding NRN arc. Some exceptions exist such as when the BO is truly not
required and is not ``untouchable`` (see :ref:`Meshblock Creation Validation 102`).

The NRN is considered the ``base`` geometry for the EGP. Therefore, when deciding which arc to modify (NGD or NRN),
modify the NGD data.

Make use of the WMS resources available within your ``.qgz`` file to avoid incorrectly touching BOs.

.. admonition:: Note

    This is a non-logged validation and is purely used to classify all unintegrated BOs and feed into the remaining
    100-series validations.

Scenario: Endpoint Snapping
"""""""""""""""""""""""""""

.. figure:: /source/_static/meshblock_creation/validation_100_endpoint_snapping.png
    :alt: Validation 100 example - endpoint snapping.

    Figure 3: Validation 100 example - endpoint snapping.

**Actions:**

1. Snap BO endpoint to NRN endpoint.

Scenario: Non-Endpoint Snapping
"""""""""""""""""""""""""""""""

.. figure:: /source/_static/meshblock_creation/validation_100_non_endpoint_snapping.png
    :alt: Validation 100 example - non-endpoint snapping.

    Figure 4: Validation 100 example - non-endpoint snapping.

**Actions:**

1. Snap BO vertex to, or create new BO vertex at, NRN vertex.
2. Split required arc(s) (BO or NRN) at point of intersection.

Scenario: Crossing Arcs
"""""""""""""""""""""""

.. figure:: /source/_static/meshblock_creation/validation_100_crossing_arcs.png
    :alt: Validation 100 example - crossing arcs.

    Figure 5: Validation 100 example - crossing arcs.

**Actions:**

1. If possible, snap BO endpoint to NRN vertex.
2. Split required arc(s) (BO, NRN, or both) at point of intersection.

Scenario: Overlapping Arcs
""""""""""""""""""""""""""

.. figure:: /source/_static/meshblock_creation/validation_100_overlapping_arcs.png
    :alt: Validation 100 example - overlapping arcs.

    Figure 6: Validation 100 example - overlapping arcs.

**Actions:**

1. Delete all BO vertices along overlapping section and snap BO endpoint to NRN vertex.
2. If required, split NRN arc at point of intersection.

Scenario: BO Not Required
"""""""""""""""""""""""""

.. figure:: /source/_static/meshblock_creation/validation_100_bo_not_required.png
    :alt: Validation 100 example - BO not required.

    Figure 7: Validation 100 example - BO not required.

**Actions:**

1. Assign the no-longer-required BO ``ngd_uid`` value to the corresponding NRN arc(s)' ``ngd_uid`` field.
2. Delete the no-longer-required BO.

Scenario: Bo-to-BO Connection
"""""""""""""""""""""""""""""

.. figure:: /source/_static/meshblock_creation/validation_100_bo-to-bo.png
    :alt: Validation 100 example - Bo-to-BO connection.

    Figure 8: Validation 100 example - BO-to-BO connection.

**Actions:**

1. If required, use the other BO integration scenarios to connect the BO to the NRN network.
2. If required, leave BO-to-BO connection point as-is.

.. admonition:: Note

    Many BOs only connect to other BOs at one or both endpoints.

Scenario: BO-to-Non-BO Connection
"""""""""""""""""""""""""""""""""

.. figure:: /source/_static/meshblock_creation/validation_100_bo-to-non-bo.png
    :alt: Validation 100 example - BO-to-Non-BO.

    Figure 9: Validation 100 example - BO-to-Non-BO.

**Actions:**

1. Copy and paste required Non-BO into NRN dataset.

    i. Select required Non-BO from NGD layer.
    ii. Edit → Copy Features.
    iii. Enable editing for the NRN layer.
    iv. With NRN layer selected: Edit → Paste Features → Save edits.

2. If required, use the other BO integration scenarios to connect the BO and Non-BO to the NRN network.
3. If required, leave BO-to-Non-BO connection point as-is.

.. admonition:: Note

    There may be several instances of Non-BOs (NGD road) missing from the NRN and worth integrating. A Non-BO should
    only be integrated if it is necessary for integrating the connected BO.

Scenario: Ferries
"""""""""""""""""

.. figure:: /source/_static/meshblock_creation/validation_100_ferries.png
    :alt: Validation 100 example - ferries.

    Figure 10: Validation 100 example - ferries.

**Actions:**

1. If required, use the other BO integration scenarios to connect the BO to the NRN network.

.. admonition:: Note

    Ferries are excluded from meshblock creation and, therefore, segmentation is not required when BOs and ferries
    intersect. However, all other :doc:`topology rules <topology_validation>` should still be respected.

Scenario: NatProvTer
""""""""""""""""""""

.. figure:: /source/_static/meshblock_creation/validation_100_natprovter.png
    :alt: Validation 100 example - NatProvTer.

    Figure 11: Validation 100 example - NatProvTer.

**Actions:**

1. If required, use the other BO integration scenarios to connect the BO to the NRN network.

.. admonition:: Note

    NatProvTer BOs are those forming any National, Provincial, and / or Territorial boundary. Avoid moving these BOs.
    Instead, try adapting the NRN geometries to these BOs. Segmentation is completely fine and doesn't count as a
    modification.

Scenario: CSD Boundary
""""""""""""""""""""""

.. figure:: /source/_static/meshblock_creation/validation_100_csd.png
    :alt: Validation 100 example - CSD boundary.

    Figure 12: Validation 100 example - CSD boundary.

**Actions:**

1. If required, use the other BO integration scenarios to connect the BO to the NRN network.

.. admonition:: Note

    CSD boundaries are important but not as strict as NatProvTer boundaries. They can be moved, just with caution and
    may need to be reviewed later on. Segmentation is completely fine and doesn't count as a modification.

Scenario: No Proper BO Connection
"""""""""""""""""""""""""""""""""

.. include:: /source/_static/meshblock_creation/validation_100_no_proper_bo_connection.rst

**Actions:**

1. Add a new BO which connects the problematic BO to the NRN network by following the route of the incorrect NGD road.
2. Set attribute ``segment_type=3``.

.. admonition:: Note

    Some instances exist where the BO-NGD network is clearly incorrect and we do not want to integrate those required
    NGD roads. This integration scenario accommodates this issue.

Scenario: CanVec Alignment
""""""""""""""""""""""""""

.. include:: /source/_static/meshblock_creation/validation_100_canvec_alignment.rst

**Actions:**

1. Avoid moving the CanVec-aligned BO. Instead, segment the BO and NRN arc(s) involved in the scenario at the point of
   intersection.

.. admonition:: Note

    BOs are aligned to CanVec 50k hydrology data. If possible, avoid moving these BOs (segmenting is acceptable). Arcs
    are allowed to cross into water.

Scenario: Unclear Connections
"""""""""""""""""""""""""""""

.. include:: /source/_static/meshblock_creation/validation_100_unclear_connections.rst

**Actions:**

1. If possible, snap BO endpoint to NRN vertex.
2. Segment the BO and NRN arc(s) involved in the scenario at the point of intersection.

.. admonition:: Note

    Unclear BO connections are acceptable if no other integration scenario is possible (i.e. no obvious BO adjustments
    possible, as above).

Scenario: Criss-Crossing BOs
""""""""""""""""""""""""""""

.. include:: /source/_static/meshblock_creation/validation_100_criss-crossing_bos.rst

**Actions:**

1. If possible, snap BO endpoint to NRN vertex.
2. Segment the BO and NRN arc(s) involved in the scenario at the point of intersection.

.. admonition:: Note

    Criss-crossing BOs and NRN arcs are acceptable if no other integration scenario is possible (i.e. no obvious BO
    adjustments possible, as shown above).

Validation 101
^^^^^^^^^^^^^^

.. figure:: /source/_static/meshblock_creation/validation_101.png
    :alt: Validation 101 example.

    Figure 13: Validation 101 example.

| **Description:** Unintegrated BO node is <= 5 meters from an NRN road (entire arc).
| **Actions:**

1. Extend / modify the BO to connect with the appropriate NRN arc(s).

.. admonition:: Warning

    Some instances of this validation may be represented by truly disconnected BOs and roads, such as BOs which
    traverse rivers or shorelines. However, those instances should be easily identifiable since the BO would not be a
    dangling arc.

.. admonition:: Note

    Enable imagery WMS layer in QGIS table of contents to assist in determining feature connectivity.

.. _Meshblock Creation Validation 102:

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

    Figure 14: Validation 201 example.

| **Description:** All non-deadend arcs (excluding ferries) must form a meshblock polygon.
| **Actions:**

1. Use the integration scenarios defined in :ref:`Meshblock Creation Validation 100` to correctly connect the arc to
   the NRN network.

.. admonition:: Note

    In this example, ``ngd_uid=4`` is flagged for not forming a meshblock polygon.

Validation 202
^^^^^^^^^^^^^^

.. figure:: /source/_static/meshblock_creation/validation_202.png
    :alt: Validation 202 example.

    Figure 15: Validation 202 example.

| **Description:** All deadend arcs (excluding ferries) must be completely within 1 meshblock polygon.
| **Actions:**

1. Use the integration scenarios defined in :ref:`Meshblock Creation Validation 100` to correctly connect the arc and
   BOs.

.. admonition:: Note

    In this example, ``segment_id=1`` is flagged for not being completely within a single meshblock polygon.
    ``segment_id=0`` is fine.
