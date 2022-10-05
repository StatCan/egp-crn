*******************
Topology Validation
*******************

.. contents:: Contents:
   :depth: 4


.. |icon_delete_selected| image:: /source/_static/topology_validation/icon_delete_selected.svg
.. |icon_merge_features| image:: /source/_static/topology_validation/icon_merge_features.svg
.. |icon_select| image:: /source/_static/topology_validation/icon_select.svg
.. |icon_split_features| image:: /source/_static/topology_validation/icon_split_features.svg

Overview
========

The CRN must be topologically correct to facilitate its usage in downstream CRN tasks in addition to satisfying NRN and
NGD specific network requirements such as routability.

Resources
---------

:CLI Tool: ``src/topology/validate_topology.py``
:Output (see data/crn.gpkg):
    - Updated source layer: ``crn_<source>``
    - Reference layers (availability conditional on validation results):
        - Cluster tolerance point layer: ``<source>_cluster_tolerance``
:Editing Environment: ``data/editing_topology.qgz``

Editing Process
---------------

.. figure:: /source/_static/topology_validation/editing_process_topology_validation.svg
    :alt: Editing process overview.

    Figure: Editing process overview.

The source layer ``crn.gpkg|layer=crn_<source>`` will contain new attributes for each validation executed by the script
(v101, v102, v301, etc.), if that validation actually returned results. The values of these attributes will be 1 or 0,
indicating whether or not that record was flagged by that validation. Use this data to edit the records.

QGIS Project
------------

Explanation of Layers
^^^^^^^^^^^^^^^^^^^^^

.. figure:: /source/_static/topology_validation/qgis_project_layers.png
    :alt: QGIS table of contents.

    Figure: QGIS table of contents.

**Layers:**

:``crn``: Primary layer representing NRN roads and ferries, NGD BOs, and added NGD roads.
:``crn``: Copy of ``crn`` with highly visible symbology for quickly identifying erroneous features to edit without
          having to individually query each one. Intended to have a definition query applied using one of the
          validation attributes (i.e. ``"v101" = 1``).
:``CanVec Hydro``: Reference WMS to help identify features which may be aligned to CanVec Hydro.
:``Esri Satellite``: Reference WMS for recent imagery context.

Validations
===========

1. Construction
---------------

Validation 101
^^^^^^^^^^^^^^

.. figure:: /source/_static/topology_validation/validation_101.png
    :alt: Validation 101 example.

    Figure: Validation 101 example.

| **Description:** Arcs must be single part (i.e. "LineString").
| **Actions:**

1. Do nothing. This is resolved automatically by the script.

Validation 102
^^^^^^^^^^^^^^

.. figure:: /source/_static/topology_validation/validation_102.png
    :alt: Validation 102 example.

    Figure: Validation 102 example.

| **Description:** Arcs must be >= 3 meters in length, except structures (e.g. Bridges).
| **Actions:**

A. If feature is bounded by 2 intersections: do nothing.
B. If feature is a dead end and is connected to 1 intersection: do nothing.
C. If feature is not connected to any other feature: delete feature.

    1. |icon_select| Select feature.
    2. |icon_delete_selected| Delete selected feature.

D. Else: merge feature with 1 of its neighbours.

    1. |icon_select| Select feature and one of its neighbours.
    2. |icon_merge_features| Merge features: Edit → Edit Geometry → Merge Selected Features → Ok.

.. admonition:: Definition

    Intersection: merging point of 3 or more arcs.

Validation 103
^^^^^^^^^^^^^^

.. figure:: /source/_static/topology_validation/validation_103.png
    :alt: Validation 103 example.

    Figure: Validation 103 example.

| **Description:** Arcs must be simple (i.e. must not self-overlap, self-cross, nor touch their interior).
| **Actions:**

A. Self-cross: delete / edit crossed segment.

    1. Add vertex to the cross point, unless vertex already exists.
    2. For 1 of the 2 crossed segments, delete all vertices beyond the cross point.
    3. For the now-disconnected neighbouring feature, add vertices to the end of the feature to recreate the deleted
       vertices.

B. Self-overlap: delete overlap.

    1. Delete duplicated vertices until segments no longer overlap.

C. Touch interior: ensure vertex is duplicated.

    1. Add vertex to segment being touched, at touch point.

**Demos:** :doc:`View video demos <demos/topology_validation_demos>`.

Validation 104
^^^^^^^^^^^^^^

.. figure:: /source/_static/topology_validation/validation_104.png
    :alt: Validation 104 example.

    Figure: Validation 104 example.

| **Description:** Arcs must have >= 0.01 meters distance between adjacent vertices (cluster tolerance).
| **Actions:**

1. Delete as many vertices as required, until no 2 adjacent vertices are within the cluster tolerance.

2. Duplication
--------------

Validation 201
^^^^^^^^^^^^^^

.. figure:: /source/_static/topology_validation/validation_201.png
    :alt: Validation 201 example.

    Figure: Validation 201 example.

| **Description:** Arcs must not be duplicated.
| **Actions:**

1. Delete all but 1 of the duplicated features.

Validation 202
^^^^^^^^^^^^^^

.. figure:: /source/_static/topology_validation/validation_202.png
    :alt: Validation 202 example.

    Figure: Validation 202 example.

| **Description:** Arcs must not overlap (i.e. contain duplicated adjacent vertices).
| **Actions:**

1. For any overlapping features that continue beyond both ends of the overlap: split feature into 3.

    i. |icon_split_features| Select Split Features tool: Edit → Edit Geometry → Split Features.
    ii. Split features at beginning of overlap: draw a line across feature to split into 2 (split at the vertex to
        avoid creating new vertices).
    iii. Split feature again at end of overlap.

2. For any overlapping features that continue beyond just 1 end of the overlap: split feature into 2.
3. Now delete all but 1 of the overlapping features.

**Demos:** :doc:`View video demos <demos/topology_validation_demos>`.

3. Connectivity
---------------

Validation 301
^^^^^^^^^^^^^^

.. figure:: /source/_static/topology_validation/validation_301.png
    :alt: Validation 301 example.

    Figure: Validation 301 example.

| **Description:** Arcs must only connect at endpoints (nodes).
| **Actions:**

1. Split feature which is being intersected at a non-node into 2 features (split at the vertex to avoid creating new
   vertices).

Validation 302
^^^^^^^^^^^^^^

.. figure:: /source/_static/topology_validation/validation_302.png
    :alt: Validation 302 example.

    Figure: Validation 302 example.

| **Description:** Arcs must be >= 5 meters from each other, excluding connected arcs (i.e. no dangles).
| **Actions:**

A. If features can be confirmed as being actually connected: connect features.

    1. Add 1 or more vertices to extend and connect one of the disconnected features to the other feature.

B. Else: do nothing.

.. admonition:: Note

    Enable imagery WMS layer in QGIS table of contents to assist in determining feature connectivity.

Validation 303
^^^^^^^^^^^^^^

.. figure:: /source/_static/topology_validation/validation_303.png
    :alt: Validation 303 example.

    Figure: Validation 303 example.

| **Description:** Arcs must not cross (i.e. must be segmented at each intersection).
| **Actions:**

1. Split feature at every point where it crosses another feature.

**Demos:** :doc:`View video demos <demos/topology_validation_demos>`.

.. admonition:: Note

    If the feature being split has no vertex at the crossing point, click again when drawing the Split Features line.
