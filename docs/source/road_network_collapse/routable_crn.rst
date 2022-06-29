************
Routable CRN
************

.. contents:: Contents:
   :depth: 3

Overview
========

The CRN has been segmented at each and every arc intersection to support meshblock creation. However, this effectively
makes the network unroutable with respect to real-world connectivity between roads. Therefore, the CRN must contain a
dissolve indicator attribute on which a subset of roads which have been segmented, but are contiguous in reality, can
be merged into single features. An interim point dataset representing all crossings of a specified order should also be
maintained to simplify the update process after deltas integration phases.

.. admonition:: Crossings Order

    Order represents the number of features connected to a crossing point. In this context, 4 is the minimum order used
    to identify points to be exported to the crossings dataset because this is seen as the minimum order involving
    real-world, multi-level road crossings. Technically, it is possible to used 3 (dead end terminating directly
    beneath another road), but this would exponentially increase the number of points requiring review for only a
    handful of valid instances.

Resources
---------

:CLI Tool: ``src/routability/gen_crossings.py``
:Output (see data/egp_data.gpkg):
    - Basic metrics output to console.
    - Crossings point layer:
        - If already exists, only delta crossings: ``<source>_crossings_deltas``
        - Otherwise, all crossings: ``<source>_crossings``
:Editing Environment: ``data/egp_editing_routable_crossings.qgz``

Editing Process
---------------

.. figure:: /source/_static/routable_crn/editing_process_routable_crn.svg
    :alt: Editing process overview.

    Figure: Editing process overview.

QGIS Project
------------

Explanation of Layers
^^^^^^^^^^^^^^^^^^^^^

.. figure:: /source/_static/routable_crn/qgis_project_layers.png
    :alt: QGIS table of contents.

    Figure: QGIS table of contents.

**Layers:**

:``crossings_deltas``:
    Crossings differences relative to the existing crossings dataset.
        :Additions: New crossings.
        :Deletions: Removed crossings.
        :Modifications: Preserved crossings with a different feature count.
:``crossings``: Crossings dataset, symbolized according to dissolve requirement status.
:``crn_roads``: CRN roads, used to determine if each crossing point contains dissolvable features.
:``Esri Satellite``: Reference WMS for recent imagery context.

Output
======

Crossing Classification
-----------------------

.. include:: /source/_static/routable_crn/example_output_classification.rst

Dissolve IDs
------------

.. figure:: /source/_static/routable_crn/example_output_dissolve_ids.png
    :alt: Dissolve IDs example output.

    Figure: Dissolve IDs example output.

Progress
========

.. admonition:: Note

    This section is temporary and will be removed once this task is completed.

.. figure:: /source/_static/progress/routable_crn_progress_chart.svg
    :alt: Routable CRN progress chart.

    Figure: Routable CRN progress chart as of June 27, 2022.

.. figure:: /source/_static/progress/routable_crn_progress_map.svg
    :alt: Routable CRN progress map.

    Figure: Routable CRN progress map as of June 27, 2022.
