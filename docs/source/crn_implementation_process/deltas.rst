******
Deltas
******

.. contents:: Contents:
   :depth: 5

Overview
========

The work on the NRN and the NGD layers is continuous.
Updates, changes, and/or modifications (deltas) yield multiple periodic vintages of each dataset,
meaning that the CRN is continuously being outdated.
Therefore, these deltas must be identified, inspected, and properly integrated into the CRN for it to fully realize its
purpose as a foundational layer for the Enterprise Geospatial Platform (EGP) by providing an accurate and unified
representation of both the NRN project and NGD’s NGD_AL layer and all their dependent operations and layers (e.g. NGD_A).

Resources
---------

TODO

Editing Process
---------------

.. figure:: /source/_static/deltas/deltas_workflow_diagram.png
    :alt: Deltas process workflow.

    Figure: Deltas process workflow.

QGIS Project
------------

Explanation of Layers
^^^^^^^^^^^^^^^^^^^^^

TODO

Example Output
^^^^^^^^^^^^^^

TODO

Data Vintages
=============

Current CRN
-----------

=========================  ==================  ======================
Province / Territory       NRN Vintage (YYYY)  NGD Vintage (YYYYMMDD)
=========================  ==================  ======================
Alberta                    2022                20220210
British Columbia           2017                20210601
Manitoba                   2012                20210601
New Brunswick              2022                20220805
Newfoundland and Labrador  2012                20210601
Northwest Territories      2021                20210601
Nova Scotia                2021                20210601
Nunavut                    2021                20220210
Ontario                    2020                20210601
Prince Edward Island       2021                20210601
Quebec                     2016                20210601
Saskatchewan               2020                20210601
Yukon                      2020                20210601
=========================  ==================  ======================

Latest Available
----------------

=========================  ==================  ======================
Province / Territory       NRN Vintage (YYYY)  NGD Vintage (YYYYMMDD)
=========================  ==================  ======================
Alberta                    2022                20220805
British Columbia           2017                20220805
Manitoba                   2012                20220805
New Brunswick              2022                20220805
Newfoundland and Labrador  2012                20220805
Northwest Territories      2022                20220805
Nova Scotia                2022                20220805
Nunavut                    2022                20220805
Ontario                    2022                20220805
Prince Edward Island       2021                20220805
Quebec                     2016                20220805
Saskatchewan               2022                20220805
Yukon                      2022                20220805
=========================  ==================  ======================


Progress
========

.. admonition:: Note

    This section is temporary, it will be updated with each delta phase and will eventually be removed once this task
    is completed.

Error Counts
------------
Deltas phase (1) table of error counts
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

=========================  ==================  ======================
Province / Territory       NRN                  NGD
=========================  ==================  ======================
Alberta                    N/A                  N/A
British Columbia           N/A                  N/A
Manitoba                   40                   443
New Brunswick              345                  114
Newfoundland and Labrador  25                   154
Northwest Territories      153                  22
Nova Scotia                3756                 111
Nunavut                    7                    0
Ontario                    N/A                  N/A
Prince Edward Island       8                    58
Quebec                     N/A                  N/A
Saskatchewan               1800                 1003
Yukon                      N/A                  N/A
=========================  ==================  ======================


Progress Maps
--------------
.. figure:: /source/_static/deltas/Workload_Deltas_Phase_01.png
    :alt: Deltas Phase 1 workload.

    Figure: Deltas phase (1) workload (percentage based on the average of error counts).

.. admonition:: Note

    The inclusion of provinces and territories in a delta phase depends on their completion status with regards to the
    other CRN processes such as topology validation, meshblock creation, and meshblock conflation. Therefore,
    some provinces/territories may be excluded from a certain delta phase. For such provinces/territories,
    the integration of Deltas will be implemented in the next phases.

.. figure:: /source/_static/deltas/Progress_Deltas_Phase_01.png
    :alt: Deltas Phase 1 progress.

    Figure: Deltas integration phase (1) progress map as of 25 January, 2023 .
