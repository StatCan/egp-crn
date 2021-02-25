# National Road Network (NRN)
![GitHub release (latest by date)](https://img.shields.io/github/v/release/jessestewart1/nrn-rrn?style=flat)
[![GitHub license](https://img.shields.io/github/license/jessestewart1/nrn-rrn)](https://github.com/jessestewart1/nrn-rrn/blob/master/LICENSE.txt)
[![Documentation Status](https://readthedocs.org/projects/nrn-rrn-docs/badge/?version=latest;style=flat)](https://nrn-rrn-docs.readthedocs.io/en/latest/?badge=latest)
[![Libraries.io dependency status for GitHub repo](https://img.shields.io/librariesio/github/jessestewart1/nrn-rrn)](https://libraries.io/github/jessestewart1/nrn-rrn)
[![Anaconda-Server Badge](https://img.shields.io/badge/Install%20with-conda-green.svg?style=flat)](https://github.com/jessestewart1/nrn-rrn/blob/master/environment.yml)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/jessestewart1/nrn-rrn/HEAD)

The NRN was adopted by members from the Inter-Agency Committee on Geomatics (IACG) and the Canadian Council on Geomatics
(CCOG) to provide quality geospatial and attributive data (current, accurate, consistent), homogeneous and normalized of
the entire Canadian road network. The NRN is part of the GeoBase initiative which aims to provide a common
geospatial infrastructure that is maintained on a regular basis by closest to source organizations.

The NRN is distributed in the form of thirteen provincial / territorial datasets consisting of two linear entities
(road segments and ferry segments), three punctual entities (junctions, blocked passages, and toll points), and three
tabular entities (address ranges, street and place names, and alternative name linkages). Currently, the NRN is publicly
available on the open government data portal (https://open.canada.ca/en).

The NRN content largely conforms to ISO 14825 (https://www.iso.org/standard/54610.html).

### Table of Contents

- [Developments - NRNv3 (working title)](#developments---nrnv3--working-title-)
- [Setup](#setup)
  * [Prerequisites](#prerequisites)
  * [Installation](#installation)
- [Usage](#usage)

### Developments - NRNv3 (working title)

Since the acquisition of the NRN by Statistics Canada from Natural Resources Canada in 2018, numerous modernization 
initiatives have been conceived, facilitating significant redevelopment of the NRN. The following outlines the primary
initiatives and developments impacting the NRN:

* [X] Streamline NRN processing. Migrate away from the semi-automated and outdated processes of the inherited project.
  * [X] Develop a fully automated data processing pipeline.
  * [X] Develop a python-based project using exclusively open source tools.
  * [X] Develop a simplified feedback look with data providers (process &#8594; fix errors &#8594; process).

* [ ] NRN simplification. Simplify the NRN schema and output requirements.

* [ ] NRN - NGD integration. Redevelop the NRN to facilitate the merging of the NRN and NGD road networks into a single 
product.

* [ ] GeoBase integration. Redevelop the NRN to facilitate integration between the NRN and other GeoBase projects 
(National Address Register and National Building Layer).

* [ ] Updated documentation following NRN simplification and integration tasks.

### Setup

The pipeline is divided into fives stages where each stage is implemented as a directly callable python module, executed 
as a command line interface.

#### Prerequisites

- Anaconda with conda >= 4.9.
  - Note: the latest version of Anaconda 3 should satisfy this requirement.
  - Download: https://docs.anaconda.com/anaconda/install/
  - Validate the conda version in the command line with `conda -V`

#### Installation

1. Download and unzip the repository: https://github.com/jessestewart1/nrn-rrn

2. Create a virtual conda environment from the file `environment.yml`:

   `conda env create -f <path to environment.yml>`

3. Validate the successful creation of the virtual environment by listing all available environments:

   `conda env list`

### Usage

1. Activate the conda virtual environment:

   `conda activate nrn-rrn`

2. For each stage, navigate to the `src/stage_#` directory and use the command line interface `--help` command for 
stage-specific options:

   ```
   cd <path to project/src/stage_#>
   python stage.py --help
   ```

Example:

  ```
  C:\Windows\system32>conda activate nrn-rrn
  
  (nrn-rrn) C:\Windows\system32>cd C:/nrn-rrn/src/stage_1
  
  (nrn-rrn) C:\nrn-rrn\src\stage_1>python stage.py --help
  Usage: stage.py [OPTIONS] [ab|bc|mb|nb|nl|ns|nt|nu|on|pe|qc|sk|yt]
  
    Executes an NRN stage.
  
  Options:
    -r, --remove / --no-remove  Remove pre-existing files within the
                                data/interim directory for the specified source.
                                [default: False]
  
    --help                      Show this message and exit.
  
  (nrn-rrn) C:\nrn-rrn\src\stage_1>
  ```
