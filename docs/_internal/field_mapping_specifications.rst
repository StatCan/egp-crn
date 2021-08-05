********************************
NRN Field Mapping Specifications
********************************

.. contents::
   :depth: 4

Abbreviations
=============

.. glossary::
    NRN
        National Road Network

    UUID
        Universal Unique Identifier

    YAML
        Serialization language commonly used for configuration files.

Overview
========

Field mapping uses YAML configuration files to map source data to the NRN schema via direct 1:1 mapping or the
application of one or more data manipulation functions.

YAML Directory Structure
========================

Directory
---------

* The root directory for all field mapping YAMLs is: `nrn-rrn/src/stage_1/sources`.
* Each YAML file must exist within a subdirectory of the root using the provincial / territorial abbreviations:

    ============  ====================
    Abbreviation  Province / Territory
    ============  ====================
    ab            Alberta
    bc            British Columbia
    mb            Manitoba
    nb            New Brunswick
    nl            Newfoundland and Labrador
    ns            Nova Scotia
    nt            Northwest Territories
    nu            Nunavut
    on            Ontario
    pe            Prince Edward Island
    qc            Quebec
    sk            Saskatchewan
    yt            Yukon
    ============  ====================

Files
-----

* File names do not matter, so long as the `.yaml` extension is used.
* Each source dataset (file or layer) must use a separate YAML file.
* Each NRN dataset should only be mapped to in one YAML file per source, otherwise the results will be overwritten by 
  the last YAML file to be processed.

Examples
--------

**Generic:**

.. code-block:: yaml

  src:
    stage_1:
      sources:
        province / territory:
          source_dataset.yaml
          ...

**Specific:**

.. code-block:: yaml

  src:
    stage_1:
      sources:
        nb:
          geonb_nbrn-rrnb_ferry-traversier.yaml
          geonb_nbrn-rrnb_road-route.yaml

YAML Content
============
Field mapping YAML files consist of 3 main components: metadata, source details, and field mapping definitions.

Metadata
--------

The metadata components define all relevant details about the source data.

Structure
^^^^^^^^^

**Generic:**

.. code-block:: yaml

  coverage:
    country:
    province:
    ISO3166:
      alpha2:
      country:
      subdivision:
    website:
    update_frequency:
  license:
    url:
    text:
  language:

**Specific:**

.. code-block:: yaml

  coverage:
    country: ca
    province: nb
    ISO3166:
      alpha2: CA-NB
      country: Canada
      subdivision: New Brunswick
    website: https://geonb-t.snb.ca/downloads/nbrn/geonb_nbrn-rrnb_orig.zip
    update_frequency: weekly
  license:
    url: http://geonb.snb.ca/documents/license/geonb-odl_en.pdf
    text: GeoNB Open Data Licence
  language: en

Source
------

The source components define dataset properties relevant to constructing a (Geo)DataFrame.

Structure
^^^^^^^^^

**Generic:**

.. code-block:: yaml

  data:
    filename:
    layer:
    driver:
    crs:
    spatial:
    query:

**Specific:**

.. code-block:: yaml

  data:
    filename: 2021/geonb_nbrn-rrnb.gdb
    layer: Road_Segment_Entity
    driver: OpenFileGDB
    crs: "EPSG:2953"
    spatial: True
    query: "Functional_Road_Class != 425"

Field Mapping
-------------

The field mapping components define field mapping functions to map the source dataset to one or more NRN datasets.

Structure
^^^^^^^^^

**Generic:**

.. code-block:: yaml

  conform:
    nrn_dataset:
      nrn_dataset_field:
      ...
    ...

**Specific:**

.. code-block:: yaml

  conform:
    addrange:
      acqtech: Element_Acquisition_Technique
      metacover:
      credate: Element_Creation_Date
      datasetnam: New Brunswick
      accuracy: &accuracy
        fields: Element_Planimetric_Accuracy
        functions:
          - function: map_values
            lookup:
              401: 1
              402: 3
              403: 5
              404: 10
              405: 15
              406: 20
              407: 25
      ...
    roadseg:
      acqtech: Element_Acquisition_Technique
      metacover:
      credate: Element_Creation_Date
      datasetnam: New Brunswick
      accuracy: *accuracy
      ...
    ...

Field Mapping Details
=====================

Field Mapping Types
-------------------

None
^^^^

No source field maps to the NRN field.

**Example:**

.. code-block:: yaml

  accuracy:

Literal
^^^^^^^

A literal value maps to the NRN field.

**Example:**

.. code-block:: yaml

  accuracy: 10

Direct
^^^^^^

A source field directly maps to the NRN field.

**Example:**

.. code-block:: yaml

  accuracy: Element_Planimetric_Accuracy

Function
^^^^^^^^

One or more source fields maps to the NRN field, but requires one or more manipulations via field mapping functions.
See all available field mapping functions: `nrn-rrn\src\stage_1\field_map_functions.py`.

Example
"""""""

**Generic:**

.. code-block:: yaml

  nrn_field:
    fields: source_field or [source_field] or [source_field, source_field, ...]
    functions:
      - function: function_name
        parameter:
        ...
      - ...

**Specific 1:**

.. code-block:: yaml

  accuracy: &accuracy
    fields: Element_Planimetric_Accuracy
    functions:
      - function: map_values
        lookup:
          401: 1
          402: 3
          403: 5
          404: 10
          405: 15
          406: 20
          407: 25

**Specific 2:**

.. code-block:: yaml

  accuracy: &accuracy
    fields: First_House_Number_Left
    functions:
      - function: regex_sub
        pattern: "-"
        repl: ""
      - function: regex_find
        pattern: "(^\\d+)"
        match_index: 0
        group_index: 0

Function Field Mapping - Additional Details
-------------------------------------------

Chaining
^^^^^^^^

Multiple field mapping functions can be *chained* together as a list such that they can be executed successively. For
chained functions, the output of one function becomes the input of the next function.

Nested Source Fields
^^^^^^^^^^^^^^^^^^^^

When multiple source fields are given, they are aggregated into lists before being passed to the specified field
mapping function(s) as a Pandas Series. Certain field mapping functions expect list Series, while others do not.
Therefore, caution should be used when defining source fields and field mapping functions.

Special Parameters
^^^^^^^^^^^^^^^^^^

Process Separately
""""""""""""""""""

`process_separately`: A boolean flag (default False) to indicate if source fields should be processed through the field
mapping functions together or separately.

**Example (process_separately=True):**

.. code-block:: yaml

  placename:
    fields: [SPN_L_Place_Name, SPN_R_Place_Name]
    process_separately: True
    functions:
      - function: map_values
        lookup:
          1: Aboujagane
          2: Acadie Siding
          3: Acadieville
          ...

**Example (process_separately=False):**

.. code-block:: yaml

  l_stname_c:
    fields: [L_Directional_Prefix, L_Type_Prefix, L_Article, L_Name_Body, L_Type_Suffix, L_Directional_Suffix]
    functions:
    - function: concatenate
      columns: [dirprefix, strtypre, starticle, namebody, strtysuf, dirsuffix]
      separator: " "

Iterate Columns
"""""""""""""""

`iterate_cols`: A list of integers representing the indexes of the specified source fields which should be passed
through the specified function. This parameter is used to iterate the processing of a list Series similar to
`process_separately` while within a function chain that contains both functions which support and do not support list
Series. Values at those indexes not defined by `iterate_cols` will retain their original value.

**Example:**

.. code-block:: yaml

  l_stname_c:
    fields: [L_Directional_Prefix, L_Type_Prefix, L_Article, L_Name_Body, L_Type_Suffix, L_Directional_Suffix]
    functions:
    - function: map_values
      iterate_cols: [0, 5]
      lookup:
        1: North
        2: South
        3: East
        4: West
    - function: concatenate
      columns: [dirprefix, strtypre, starticle, namebody, strtysuf, dirsuffix]
      separator: " "

Field Domains
"""""""""""""

`domain_nrn_dataset_nrn_field`: A keyword within regular expressions which, if detected, will be substituted with a
list of domain values for the specified NRN dataset and field, concatenated by the regular expression "or" operator:
`|`.
Regular expressions can be validated using this resource: `regular expressions 101 <https://regex101.com/>`_.

**Example:**

.. code-block:: yaml

  dirprefix:
    fields: strtypre
    functions:
    - function: regex_find
      pattern: "\\b(domain_strplaname_dirprefix)\\b(?!$)"
      match_index: 0
      group_index: 0

The above field mapping definition would be converted to the following due to the field domains keyword:

.. code-block:: yaml

  dirprefix:
    fields: strtypre
    functions:
    - function: regex_find
      pattern: "\\b(None|North|Nord|South|Sud|East|Est|West|Ouest|Northwest|Nord-ouest|Northeast|Nord-est|Southwest|Sud-ouest|Southeast|Sud-est|Central|Centre)\\b(?!$)"
      match_index: 0
      group_index: 0

Missing table linkages
======================

Primary and foreign key NRN fields which do not have any source field mapping to them will be set to the default field
value. If this field must be populated, use "uuid" as the mapping field. Each NRN dataset is created with a "uuid"
field representing a unique identifier which is maintained throughout the entire NRN pipeline, therefore, it can be
used in place of missing field values which must be unique.
