********************
Meshblock Conflation
********************

.. contents:: Contents:
   :depth: 5

Overview
========

The NGD network needs to be linkable to the new CRN network. This includes individual arcs and BB (meshblock) units.

The CRN and NGD network differences are not limited to the quantity of arcs, but also the shape and alignment of those
arcs. This is why the networks cannot be simply linked arc-to-arc, but require more detailed conflation to reduce
alignment differences. A threshold of 80% is currently used to determine acceptable network alignment and classify
individual meshblock polygons (BBs) as ``conflated`` or ``unconflated``.

.. admonition:: Definition

    ``conflated``: An NGD BB which has >= 80% of its area contained within a single EGP BB.

.. admonition:: Threshold Strictness

    It will not always be possible to achieve the conflation threshold without introducing needless complexity to the
    CRN. Therefore, the threshold should be treated as a soft restriction and lower values are acceptable so long as
    the NGD BB can be guaranteed to link to a single EGP BB based on majority area occupation, as defined above for
    ``conflation``.

The required actions to resolve ``unconflated`` BBs should follow those outlined in the various validations in
:doc:`meshblock_creation`.

Resources
---------

:CLI Tool: ``src/conflation/conflate_meshblock.py``
:Output (see data/egp_data.gpkg):
    - Basic metrics output to console.
    - Updated source layer: ``nrn_bo_<source>``
    - New (EGP) BB layer: ``<source>_meshblock``
    - Current (NGD) BB layer: ``<source>_meshblock_ngd``
:Editing Environment: ``data/egp_editing_meshblock_conflation.qgz``

Editing Process
---------------

.. figure:: /source/_static/meshblock_conflation/editing_process_meshblock_conflation.svg
    :alt: Editing process overview.

    Figure: Editing process overview.

Cardinalities
-------------

.. admonition:: Definition

    Cardinalities: In a database context, cardinalities refer to the numerical relationships between records of two or
    more datasets.

.. figure:: /source/_static/meshblock_conflation/cardinalities_overview.png
    :alt: Cardinalities overview.

    Figure: Cardinalities overview. Direction: EGP (blue) - to - NGD (pink).

**Cardinalities Explained:**

``one-to-one``:
    :Explanation: Networks align.
    :Action: None.
``many-to-one``:
    :Explanation: CRN is more detailed.
    :Action: None.
``one-to-many``:
    :Explanation: NGD is more detailed.
    :Action: Create new BOs and / or copy missing NGD roads.
``many-to-many``:
    :Explanation: Networks conflict.
    :Action: Create new BOs.

QGIS Project
------------

Explanation of Layers
^^^^^^^^^^^^^^^^^^^^^

.. figure:: /source/_static/meshblock_conflation/qgis_project_layers.png
    :alt: QGIS table of contents.

    Figure: QGIS table of contents.

**Layers:**

:``nrn_bo``: Primary layer representing NRN roads and ferries, NGD BOs, and added NGD roads.
:``ngd_road``: NGD roads for reference and identifying roads missing from the CRN which are required for conflation.
:``meshblock_ngd``: Current NGD BB layer, classified by conflation validity status and labelled with a conflation
                    percentage.
:``meshblock``: New EGP BB layer generated from ``nrn_bo``, classified according to NGD BB linkage status.
:``Esri Satellite``: Reference WMS for recent imagery context.

Example Output
^^^^^^^^^^^^^^

.. include:: /source/_static/meshblock_conflation/qgis_example_output.rst

Conflation Scenarios
====================

.. admonition:: Converting NGD roads to BOs

    If an NGD road needs to be converted to a BO, copy and paste the NGD feature(s) into the CRN data and set
    ``bo_new=1``. The script will automatically set ``segment_type=3`` for these features, or you can do it yourself.

.. admonition:: Adding new BOs (completely new arcs)

    If a network difference prevents an EGP BB from being properly conflated, you may need to add a new arc to the CRN
    data. After creating the arc, either set ``bo_new=1``, ``segment_type=3``, or both. The script will automatically
    resolve the other attribute if only one of them is set.

Scenario: Missing NGD Roads
---------------------------

.. include:: /source/_static/meshblock_conflation/scenario_missing_ngd_roads.rst

Scenario: Missing False NGD Road (1)
------------------------------------

.. include:: /source/_static/meshblock_conflation/scenario_missing_false_ngd_road_1.rst

Scenario: Missing False NGD Road (2)
------------------------------------

.. include:: /source/_static/meshblock_conflation/scenario_missing_false_ngd_road_2.rst

Scenario: Misaligned Networks
-----------------------------

.. admonition:: Note

    Misaligned networks are difficult to resolve since there is no clear nor obvious solution. If the EGP and NGD BBs
    are not too different in shape, try modifying the BO shape slightly or rerunning the script with a slightly lower
    threshold value. Larger differences may require adding NGD roads as BOs and / or adding completely new BOs, thereby
    segmenting the data until the conflation threshold is satisfied.

Scenario: Misaligned Networks (1)
"""""""""""""""""""""""""""""""""

.. include:: /source/_static/meshblock_conflation/scenario_misaligned_networks_1.rst

Scenario: Misaligned Networks (2)
"""""""""""""""""""""""""""""""""

.. include:: /source/_static/meshblock_conflation/scenario_misaligned_networks_2.rst

Scenario: Misaligned Networks (3)
"""""""""""""""""""""""""""""""""

.. include:: /source/_static/meshblock_conflation/scenario_misaligned_networks_3.rst

Scenario: Misaligned Networks - Additional Examples
---------------------------------------------------

.. include:: /source/_static/meshblock_conflation/scenario_misaligned_networks_additional_examples.rst

Scenario: Structural Differences
--------------------------------

.. admonition:: Note

    A structural difference is where the CRN and NGD have different network representations (not the same as offsets /
    alignment issues).

Scenario: Structural Differences (1)
------------------------------------

.. include:: /source/_static/meshblock_conflation/scenario_structural_differences_1.rst

Scenario: Structural Differences (2)
------------------------------------

.. include:: /source/_static/meshblock_conflation/scenario_structural_differences_2.rst

Scenario: Isolated Blocks
-------------------------

.. include:: /source/_static/meshblock_conflation/scenario_isolated_blocks.rst

Scenario: Correction of NGD Road
--------------------------------

.. admonition:: Note

    Inaccurate / low resolution roads can be corrected based on the imagery layer, if desired, so long as the
    conflation threshold remains satisfied. However, this is not always possible (as shown below) and is not the
    objective of this task.

.. include:: /source/_static/meshblock_conflation/scenario_correction_of_ngd_road.rst

Progress
========

.. admonition:: Note

    This section is temporary and will be removed once this task is completed.

.. figure:: /source/_static/progress/meshblock_conflation_progress_chart.svg
    :alt: Meshblock conflation progress chart.

    Figure: Meshblock conflation progress chart as of July 25, 2022.

.. figure:: /source/_static/progress/meshblock_conflation_progress_map.svg
    :alt: Meshblock conflation progress map.

    Figure: Meshblock conflation progress map as of July 25, 2022.
