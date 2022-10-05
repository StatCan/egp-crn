***********
Environment
***********

.. contents:: Contents:
   :depth: 4


.. |icon_editing_enable| image:: /source/_static/environment/icon_editing_enable.svg
.. |icon_editing_save| image:: /source/_static/environment/icon_editing_save.svg
.. |icon_select| image:: /source/_static/environment/icon_select.svg
.. |icon_snapping_advanced| image:: /source/_static/environment/icon_snapping_advanced.svg
.. |icon_snapping_enable| image:: /source/_static/environment/icon_snapping_enable.svg
.. |icon_snapping_intersection| image:: /source/_static/environment/icon_snapping_intersection.svg
.. |icon_snapping_vertex| image:: /source/_static/environment/icon_snapping_vertex.svg
.. |icon_vertex_enable_editing| image:: /source/_static/environment/icon_vertex_enable_editing.svg

Virtual Machine (VM) Access
===========================

All work is done on a personalized VM on the StatCan cloud environment. Your VM is named according to the format:
``egp-<username>``. See the following instructions for accessing your VM:
:download:`Download PDF </source/_static/environment/vm_access.pdf>`

Software
========

All software dependencies are pre-installed. All you need is the following:

Data Editing
------------

- `QGIS <https://www.qgis.org/en/site/forusers/download.html>`_: Open source GIS application.

Repository Management and Script Usage
--------------------------------------

- `Git <https://git-scm.com/downloads>`_: Version control system for tracking code changes and collaborative
  development.
- `conda <https://docs.anaconda.com/anaconda/install/>`_: Virtual environment and package manager.

Repository
==========

The repository is the root directory containing all files and code for a project. This project's repository is named
``egp-crn`` and is already installed on your VM at ``C:/egp-crn``.

``Git`` is used for repository management. ``Git`` allows you to fetch content from a remote repository (GitHub in this
case) and integrate the differences into your local repository.

Installation
------------

1. Change directory to the desired installation location::

    cd /d C:/

2. Install the repository::

    git clone https://github.com/StatCan/egp-crn.git

Updates
-------

1. Change directory to the repository root::

    cd /d C:/egp-crn

2. Fetch and integrate updates::

    git pull

Virtual Environment
===================

All scripts within the ``egp-crn`` repository are intended to be executed within a ``conda`` virtual environment. The
``conda`` environment is defined within an ``environment.yml`` file within the ``egp-crn`` repository and is already
installed on your VM.

``conda`` is an environment and package manager and is used by the ``egp-crn`` repository to provide an isolated
processing environment and effective dependency management. The ``conda`` environment must be activated before
executing any scripts in order to make use of the contained dependencies.

Installation
------------

Install the ``conda`` environment via::

    conda env create -f C:/egp-crn/environment.yml

Activation
----------

Activate the ``conda`` environment via::

    conda activate egp-crn

Updates
-------

Update the ``conda`` environment via (only required if dependencies change)::

    conda env update -f C:/egp-crn/environment.yml --prune

Scripts
=======

All scripts within the ``egp-crn`` repository are implemented as CLI tools and can be called from any shell. The
specific parameters and details of each CLI tool can be viewed by passing the keyword ``--help``.

Script usage:

1. Activate the ``conda`` environment::

    conda activate egp-crn

2. Change directory to the script location::

    cd /d C:/egp-crn/src/topology

3. Execute the script CLI:

  a. Regular execution example::

      python validate_topology.py bc

  b. Execution to view parameter details::

      python validate_topology.py --help

.. figure:: /source/_static/environment/script_usage.gif
    :alt: Script CLI execution demo.

    Figure: Script CLI execution demo.

QGIS
====

Basic Editing Actions
---------------------

Enable / Disable Editing
^^^^^^^^^^^^^^^^^^^^^^^^

1. |icon_editing_enable| Enable editing.

2. |icon_select| Select arc(s) and perform edits.

3. |icon_editing_save| Save edits.

4. |icon_editing_enable| Disable editing.

Enable Snapping
^^^^^^^^^^^^^^^

1. |icon_snapping_enable| Enable snapping.

2. |icon_snapping_advanced| Open snapping options → Advanced Configuration.

3. Check box to enable snapping for specific layers.

4. |icon_snapping_vertex| Enable vertex snapping for specific layers: Type → Vertex.

5. |icon_snapping_intersection| Enable Snapping on Intersection.

Add / Move / Delete Vertex
^^^^^^^^^^^^^^^^^^^^^^^^^^

1. |icon_vertex_enable_editing| With editing enabled, open vertex editor.

2. Right-click arc to display vertices in vertex editor.

3. Edit vertices:

  a. Add: Hover over arc and click plus (+) icon, click again at desired location to place vertex.

  b. Move: Click a vertex and click again at desired location to place vertex.

  c. Delete: Select vertex (clicking and drag cursor or select from vertex editor) and press <Delete> key.

QGIS Editing Demo
^^^^^^^^^^^^^^^^^

.. raw:: html

    <video controls src="../../_static/environment/qgis_basic_editing_actions.mp4" type="video/mp4" width=100%></video>

Updating Layer Sources
----------------------

QGIS project files (``.qgz``) are part of the repository and, therefore, modifications will be included with any
:code:`git pull` that you perform.

:Problem: Your data sources will be overwritten by those being used when the modifications were made.
:Solution: For each layer, right-click → Change Data Source...

.. figure:: /source/_static/environment/qgis_updating_layer_sources.png
    :alt: QGIS - Updating layer sources.

    Figure: QGIS - Updating layer sources.

Keyboard Shortcuts (Hotkeys)
----------------------------

Hotkeys are recommended to make editing activities less tedious and can be assigned to individual QGIS actions via:
Settings → Keyboard Shortcuts... → Search / select action → Change → Press desired keyboard key.

.. figure:: /source/_static/environment/qgis_keyboard_shortcuts.png
    :alt: QGIS - Assigning hotkeys.

    Figure: QGIS - Assigning hotkeys.
