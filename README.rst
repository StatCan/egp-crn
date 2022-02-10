Enterprise Geospatial Platform (EGP)
====================================

.. image:: https://img.shields.io/badge/Repository-egp-brightgreen.svg?style=flat-square&logo=github
   :target: https://github.com/StatCan/egp
.. image:: https://img.shields.io/badge/License-BSD%203--Clause-blue.svg?style=flat-square
   :target: https://opensource.org/licenses/BSD-3-Clause

Description
-----------

Repository for the Enterprise Geospatial Platform and its constituent projects. The current projects in this repository
are:

:Canadian Road Network (CRN): Collapse of the National Road Network (NRN) and National Geographic Database (NGD) Road
                              Network File (RNF).

.. admonition:: Note

    The CRN is the only project currently in the EGP repository and, therefore occupies the ``master`` branch. Once
    more projects join the EGP repository, each project will occupy its own branch.

Setup
-----

The repository of the EGP project is referred to by its actual repository name: ``egp``.

Software Dependencies
^^^^^^^^^^^^^^^^^^^^^

The ``egp`` has no mandatory software dependencies but highly recommends the software specified in this section.
Furthermore, documentation for ``egp`` installation and usage will make use of this software since it represents the
easiest and recommended approach.

Anaconda / conda
""""""""""""""""

The ``egp`` is written in pure Python, but has several dependencies written with C libraries. These C libraries can be
difficult to install (particularly on Windows) and, therefore, it is recommended to create and use the conda virtual
environment defined in the ``egp``. conda is an environment and package manager and is the preferable choice for
dependency management since it provides pre-built binaries for all dependencies of the ``egp`` for all platforms
(Windows, Mac, Linux).

| `Anaconda <https://docs.anaconda.com/anaconda/install/>`_ with conda >= 4.9.
| `Miniconda <https://docs.conda.io/en/latest/miniconda.html>`_ will also suffice (minimal distribution only containing
  Python and conda).

Git
"""

| `Git <https://git-scm.com/downloads>`_ is recommended for simpler repository installation and integration of updates.

Installation
^^^^^^^^^^^^

Use the following steps to install the ``egp`` repository and conda environment:

1. Install the repository.

  a) Using Git::

      git clone https://github.com/StatCan/egp.git

  b) Manual install: Download and unzip the `repository <https://github.com/StatCan/egp>`_.

2. Create the conda environment from the ``environment.yml`` file::

    conda env create -f egp/environment.yml
