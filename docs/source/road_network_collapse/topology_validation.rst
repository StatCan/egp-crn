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

The NRN must be topologically correct to facilitate its usage in downstream EGP tasks in addition to satisfying road
network requirements such as routability.

Resources
---------

:CLI Tool: ``egp/src/topology/validate_topology.py``
:Output: ``egp/data/interim/validations.log``
:Editing Environment: ``egp/data/interim/egp_editing_topology.qgz``

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

.. figure:: /source/_static/topology_validation/editing_process_topology_validation.png
    :alt: Editing process overview.

    Figure 1: Editing process overview.

Validations
===========

1. Construction
---------------

Validation 101
^^^^^^^^^^^^^^

.. figure:: /source/_static/topology_validation/validation_101.png
    :alt: Validation 101 example.

    Figure 2: Validation 101 example.

| **Description:** Arcs must be single part (i.e. "LineString").
| **Actions:**

1. Do nothing. This is resolved automatically by the script.

Validation 102
^^^^^^^^^^^^^^

.. figure:: /source/_static/topology_validation/validation_102.png
    :alt: Validation 102 example.

    Figure 3: Validation 102 example.

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

    Figure 4: Validation 103 example.

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

    Figure 5: Validation 104 example.

| **Description:** Arcs must have >= 0.01 meters distance between adjacent vertices (cluster tolerance).
| **Actions:**

1. Delete as many vertices as required, until no 2 adjacent vertices are within the cluster tolerance.

2. Duplication
--------------

Validation 201
^^^^^^^^^^^^^^

.. figure:: /source/_static/topology_validation/validation_201.png
    :alt: Validation 201 example.

    Figure 6: Validation 201 example.

| **Description:** Arcs must not be duplicated.
| **Actions:**

1. Delete all but 1 of the duplicated features.

Validation 202
^^^^^^^^^^^^^^

.. figure:: /source/_static/topology_validation/validation_202.png
    :alt: Validation 202 example.

    Figure 7: Validation 202 example.

| **Description:** Arcs must not overlap (i.e. contain duplicated adjacent vertices).
| **Actions:**

1. For any overlapping features that continue beyond both ends of the overlap: split feature into 3.
    1. |icon_split_features| Select Split Features tool: Edit → Edit Geometry → Split Features.
    2. Split features at beginning of overlap: draw a line across feature to split into 2 (split at the vertex to avoid
       creating new vertices).
    3. Split feature again at end of overlap.
2. For any overlapping features that continue beyond just 1 end of the overlap: split feature into 2.
3. Now delete all but 1 of the overlapping features.

**Demos:** :doc:`View video demos <demos/topology_validation_demos>`.

3. Connectivity
---------------

Validation 301
^^^^^^^^^^^^^^

.. figure:: /source/_static/topology_validation/validation_301.png
    :alt: Validation 301 example.

    Figure 8: Validation 301 example.

| **Description:** Arcs must only connect at endpoints (nodes).
| **Actions:**

1. Split feature which is being intersected at a non-node into 2 features (split at the vertex to avoid creating new
   vertices).

Validation 302
^^^^^^^^^^^^^^

.. figure:: /source/_static/topology_validation/validation_302.png
    :alt: Validation 302 example.

    Figure 9: Validation 302 example.

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

    Figure 10: Validation 303 example.

| **Description:** Arcs must not cross (i.e. must be segmented at each intersection).
| **Actions:**

1. Split feature at every point where it crosses another feature.

**Demos:** :doc:`View video demos <demos/topology_validation_demos>`.

.. admonition:: Note

    If the feature being split has no vertex at the crossing point, click again when drawing the Split Features line.
