# National Road Network (NRN)
![GitHub release (latest by date)](https://img.shields.io/github/v/release/jessestewart1/nrn-rrn?style=flat)
[![GitHub license](https://img.shields.io/github/license/jessestewart1/nrn-rrn)](https://github.com/jessestewart1/nrn-rrn/blob/master/LICENSE.txt)
[![Documentation Status](https://readthedocs.org/projects/nrn-rrn-docs/badge/?version=latest;style=flat)](https://nrn-rrn-docs.readthedocs.io/en/latest/?badge=latest)
[![Libraries.io dependency status for GitHub repo](https://img.shields.io/librariesio/github/jessestewart1/nrn-rrn)](https://libraries.io/github/jessestewart1/nrn-rrn)
[![Anaconda-Server Badge](https://img.shields.io/badge/Install%20with-conda-green.svg?style=flat)](https://github.com/jessestewart1/nrn-rrn/blob/master/environment.yml)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/jessestewart1/nrn-rrn/HEAD)

## Table of Contents

- [Brief](#brief)
- [Developments and Improvements](#developments-and-Improvements)
- [Setup](#setup)
  * [Prerequisites](#prerequisites)
  * [Installation](#installation)
- [Usage](#usage)

## Brief

The NRN was adopted by members from the Inter-Agency Committee on Geomatics (IACG) and the Canadian Council on Geomatics
(CCOG) to provide quality geospatial and attributive data (current, accurate, consistent), homogeneous and normalized of
the entire Canadian road network. The NRN is part of the GeoBase initiative which aims to provide a common
geospatial infrastructure that is maintained on a regular basis by closest to source organizations.

The NRN is distributed in the form of thirteen provincial / territorial datasets consisting of two linear entities
(road segments and ferry segments), three punctual entities (junctions, blocked passages, and toll points), and three
tabular entities (address ranges, street and place names, and alternative name linkages). Currently, the NRN is publicly
available on the open government data portal (https://open.canada.ca/en).

The NRN content largely conforms to ISO 14825 (https://www.iso.org/standard/54610.html).

## Developments and Improvements

Since the acquisition of the NRN by Statistics Canada from Natural Resources Canada in 2018, numerous modernization 
initiatives have been conceived, facilitating a complete redevelopment of the NRN processing pipeline. The most 
significant NRN developments include:
- Completely open source and python-based processing pipeline.
- Command-line interface (CLI) implementation with the ability for full and partial-automation (users can choose to run 
the entire pipeline or just individual parts).
- Simplified installation via conda virtual environment (no setup / configuration issues).
- Streamlined documentation maintenance, including translations, via Sphinx and reStructuredText (.rst).
- Integrated and flexible data harmonization: configuration YAMLs define builtin functions to standardize source data 
into NRN format.
- Compatibility with almost all file formats (all GDAL vector drivers), including Linear Reference Systems (LRS).
- Optimized data validations: clear and informative error logging and a validation lookup document containing 
standardized error codes and their detailed explanations.
- Optimized data processing: the smallest dataset (Prince Edward Island) takes < 5 minutes; the largest dataset 
(Ontario) takes < 1.5 hours.

## Setup

The pipeline is divided into fives stages where each stage is implemented as a directly callable python module, executed 
as a command-line interface.

### Prerequisites

- Anaconda with conda >= 4.9.
  - Note: the latest version of Anaconda 3 should satisfy this requirement.
  - Download: https://docs.anaconda.com/anaconda/install/
  - Validate the conda version in the command line with `conda -V`

### Installation

1. Download and unzip the repository: https://github.com/jessestewart1/nrn-rrn

2. Create a virtual conda environment from the file `environment.yml`:

   `conda env create -f <path to environment.yml>`

3. Validate the successful creation of the virtual environment by listing all available environments:

   `conda env list`

## Usage

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
