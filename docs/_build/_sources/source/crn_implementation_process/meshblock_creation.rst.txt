******************
Meshblock Creation
******************

.. contents:: Contents:
   :depth: 5

Overview
========

A new meshblock (polygon units, also known as ``basic blocks`` (``BB``s)) needs to be developed based on the CRN using
both NRN roads and NGD BOs (boundary-only arcs) as inputs. The resulting polygon network will be the foundational layer
of the entire hierarchy of statistical geographic units and will replace NGD's NGD_AL dataset.

Resources
---------

:CLI Tool: ``src/meshblock/validate_meshblock.py``
:Output (see data/crn.gpkg):
    - Basic metrics output to console.
    - Updated source layer: ``crn_<source>``
    - Reference layers (availability conditional on validation results):
        - Missing BOs layer: ``<source>_missing_bo``
        - Deadend points layer: ``<source>_deadends``
        - New (CRN) BB layer: ``<source>_meshblock``
:Editing Environment: ``data/editing_meshblock.qgz``

Editing Process
---------------

.. figure:: /source/_static/meshblock_creation/editing_process_meshblock_creation.svg
    :alt: Editing process overview.

    Figure: Editing process overview.

The source layer ``crn.gpkg|layer=crn_<source>`` will contain new attributes for each validation executed by the script
(v101, v102, v301, etc.), if that validation actually returned results. The values of these attributes will be 1 or 0,
indicating whether or not that record was flagged by that validation. Use this data to edit the records.

QGIS Project
------------

Explanation of Layers
^^^^^^^^^^^^^^^^^^^^^

.. figure:: /source/_static/meshblock_creation/qgis_project_layers.png
    :alt: QGIS table of contents.

    Figure: QGIS table of contents.

**Layers:**

:``crn``: Primary layer representing NRN roads, NGD BOs, and added NGD roads.
:``ngd_road``: NGD roads for reference and identifying roads missing from the CRN which are required for BO integration.
:``crn``: Copy of ``crn`` with highly visible symbology for quickly identifying erroneous features to edit without
          having to individually query each one. Intended to have a definition query applied using one of the
          validation attributes (i.e. ``"v101" = 1``).
:``Esri Satellite``: Reference WMS for recent imagery context.

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

Every BO (boundary-only) arc must be integrated into the CRN, whether it be the actual arc itself or just the
assignment of the identifier to a corresponding NRN arc.

The NRN is considered the ``base`` geometry for the CRN. Therefore, when deciding which arc to modify (NGD or NRN),
modify the NGD data.

Make use of the WMS resources available within your ``.qgz`` file to avoid incorrectly touching BOs.

.. admonition:: Note

    This is a non-logged validation and is purely used to classify all unintegrated BOs and feed into the remaining
    100-series validations.

Scenario: Endpoint Snapping
"""""""""""""""""""""""""""

.. figure:: /source/_static/meshblock_creation/validation_100_endpoint_snapping.png
    :alt: Validation 100 example - endpoint snapping.

    Figure: Validation 100 example - endpoint snapping.

**Actions:**

1. Snap BO endpoint to NRN endpoint.

Scenario: Non-Endpoint Snapping
"""""""""""""""""""""""""""""""

.. figure:: /source/_static/meshblock_creation/validation_100_non_endpoint_snapping.png
    :alt: Validation 100 example - non-endpoint snapping.

    Figure: Validation 100 example - non-endpoint snapping.

**Actions:**

1. Snap BO vertex to, or create new BO vertex at, NRN vertex.
2. Split required arc(s) (BO or NRN) at point of intersection.

Scenario: Crossing Arcs
"""""""""""""""""""""""

.. figure:: /source/_static/meshblock_creation/validation_100_crossing_arcs.png
    :alt: Validation 100 example - crossing arcs.

    Figure: Validation 100 example - crossing arcs.

**Actions:**

1. If possible, snap BO endpoint to NRN vertex.
2. Split required arc(s) (BO, NRN, or both) at point of intersection.

Scenario: Overlapping Arcs
""""""""""""""""""""""""""

.. figure:: /source/_static/meshblock_creation/validation_100_overlapping_arcs.png
    :alt: Validation 100 example - overlapping arcs.

    Figure: Validation 100 example - overlapping arcs.

**Actions:**

1. Delete all BO vertices along overlapping section and snap BO endpoint to NRN vertex.
2. If required, split NRN arc at point of intersection.

Scenario: Obsolete BO
"""""""""""""""""""""

.. figure:: /source/_static/meshblock_creation/validation_100_obsolete_bo.png
    :alt: Validation 100 example - Obsolete BO.

    Figure: Validation 100 example - Obsolete BO.

**Actions:**

1. Assign the obsolete BO ``ngd_uid`` value to the corresponding NRN arc(s)' ``ngd_uid`` field.
2. Delete the obsolete BO.

.. admonition:: Note

    If the BO only covers a small portion of an NRN arc, you may wish to split the associated arc to avoid over
    representing the ``ngd_uid`` by a much larger feature.

Scenario: BO-to-BO Connection
"""""""""""""""""""""""""""""

.. figure:: /source/_static/meshblock_creation/validation_100_bo-to-bo.png
    :alt: Validation 100 example - Bo-to-BO connection.

    Figure: Validation 100 example - BO-to-BO connection.

**Actions:**

1. If required, use the other BO integration scenarios to connect the BO to the CRN.
2. If required, leave BO-to-BO connection point as-is.

.. admonition:: Note

    Many BOs only connect to other BOs at one or both endpoints.

Scenario: Missing NGD Road
""""""""""""""""""""""""""

.. figure:: /source/_static/meshblock_creation/validation_100_missing_ngd_road.png
    :alt: Validation 100 example - Missing NGD road.

    Figure: Validation 100 example - Missing NGD road.

**Actions:**

1. Copy and paste required NGD road into the CRN dataset.

    i. Select required NGD road from NGD layer.
    ii. Edit → Copy Features.
    iii. Enable editing for the CRN layer.
    iv. With CRN layer selected: Edit → Paste Features → Save edits.

2. If required, use the other BO integration scenarios to connect the BO and NGD road to the CRN.

.. admonition:: Note

    There may be several instances of missing NGD roads, but try to limit your integration of these roads exclusively
    to those required for resolving BO connections.

.. admonition:: Converting NGD roads to BOs

    If an NGD road needs to be converted to a BO, copy and paste the NGD feature(s) into the CRN data and set
    ``bo_new=1``. The script will then automatically set the correct ``segment_type`` for these features.

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

    Figure: Validation 101 example.

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

| **Description:** BO identifier is missing.
| **Actions:**

1. Assign the missing BO identifier (``ngd_uid``) to the appropriate arc(s) or restore the missing arc completely from
   the ``<source>_missing_bo`` layer.

.. admonition:: Note

    Missing BOs: BO geometries can be modified and even deleted (if replaced by an NRN road), but the identifier
    (``ngd_uid``) must still exist in the dataset.

2. Meshblock
------------

Validation 201
^^^^^^^^^^^^^^

.. figure:: /source/_static/meshblock_creation/validation_201.png
    :alt: Validation 201 example.

    Figure: Validation 201 example.

| **Description:** All non-deadend arcs must form a meshblock polygon.
| **Actions:**

1. Use the integration scenarios defined in :ref:`Meshblock Creation Validation 100` to correctly connect the arc to
   the CRN.

.. admonition:: Note

    In this example, ``ngd_uid=4`` is flagged for not forming a meshblock polygon.

Validation 202
^^^^^^^^^^^^^^

.. figure:: /source/_static/meshblock_creation/validation_202.png
    :alt: Validation 202 example.

    Figure: Validation 202 example.

| **Description:** All deadend arcs must be completely within 1 meshblock polygon.
| **Actions:**

1. Use the integration scenarios defined in :ref:`Meshblock Creation Validation 100` to correctly connect the arc and
   BOs.

.. admonition:: Note

    In this example, ``segment_id=1`` is flagged for not being completely within a single meshblock polygon.
    ``segment_id=0`` is fine.

Progress
========

.. admonition:: Note

    This section is temporary and will be removed once this task is completed.

.. figure:: /source/_static/progress/meshblock_creation_workload_map.png
    :alt: Meshblock creation workload.

    Figure: Meshblock creation workload (error counts).

.. figure:: /source/_static/progress/meshblock_creation_progress_map.png
    :alt: Meshblock creation progress map.

    Figure: Meshblock creation progress map as of March 13, 2023.
