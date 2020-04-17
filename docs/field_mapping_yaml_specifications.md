# DRAFT COPY

# Overview
Field mapping uses YAML configuration files to map an incoming data source to NRN (target) dataset fields.

# YAML Directory Structure
- The root directory for all field mapping YAMLs is: src/stage_1/sources.
- Each data source must have a separate YAML for each dataset to be used.
    - Each dataset YAML must be located in root/province, where province = official abbreviation.
      - Official abbreviations: ab, bc, mb, nb, nl, ns, nt, nu, on, pe, qc, sk, yt.
    - YAML file names do not matter, so long as the .yaml extension is used.

Example (generic):
  src:
    stage_1:
      sources:
        province:
          source_dataset.yaml
          ...
Example (specific):
  src:
    stage_1:
      sources:
        nb:
          geonb_nbrn-rrnb_ferry-traversier.yaml
          geonb_nbrn-rrnb_road-route.yaml

# YAML Content
YAMLs have several keys, some are purely metadata while others are used for field mapping.
- Metadata: coverage, license, language.
- Field mapping: data, conform.

## Key: Data
YAML data key must have the following content:

data:
  filename: The source dataset path, relative to the raw data root: data/raw/province, where province = official abbreviation (see section "YAML Directory Structure").
  layer: The layer name within the dataset. Only required if the dataset contains layers (such as a GeoPackage).
  driver: A GDAL vector driver name.
  crs: The EPSG shorthand string for the dataset / layer.
  spatial: Boolean (True / False) flag to indicate whether the dataset / layer is spatial or tabular.

Example data source:
  data:
    raw:
      nb:
        geonb_nbrn-rrnb_shp:
          geonb_nbrn-rrnb_ferry-traversier.shp
          geonb_nbrn-rrnb_road-route.shp

Example YAML data key:
  data:
    filename: geonb_nbrn-rrnb_shp/geonb_nbrn-rrnb_road-route.shp
    layer:
    driver: ESRI Shapefile
    crs: "EPSG:2953"
    spatial: True

## Key: Conform
YAML conform key contains all of the field mapping between the source and target datasets. The generic structure is the following:

conform:
  target_dataset:
    target_field: source_field
    target_field: source_field
    ...
  target_dataset:
    target_field: source_field
    ...
  ...

Notes:
- Only target datasets which actually map the source dataset should be included.
- Target fields can be included or excluded. All target fields without a mapping will appear in the output dataset with the default field values.

### Field Mapping Types
There are several field mapping types:
1. None:
  Example:
    target_field:
2. Raw value:
  Example:
    target_field: some value
    target_field: "some value"
3. Direct:
  Example:
    target_field: source_field
4. Function: see section "YAML Content/Key: Conform/Standard Field Mapping Functions"

### Standard Field Mapping Functions
Standard field mapping functions map a function in stage_1/field_map_functions to each record in the source field(s), using a number of function-specific kwargs.

Example (generic):
  target_field:
    fields: source_field or [source_field] or [source_field, source_field, ...]
    functions:
      - function: function_name
        parameter:
        parameter:
        ...

Example:
  namebody:
    fields: name
    functions:
      - function: regex_sub
        pattern_from: "-"
        pattern_to: ""

### Chaining
Multiple functions can be chained such that the output of one becomes the input of the next.

Example:
  l_hnumf:
    fields: left_civic_from
    functions:
      - function: regex_sub
        pattern_from: "-"
        pattern_to: ""
      - function: regex_find
        pattern: "(^\\d+)"
        match_index: 0
        group_index: 0

#### Special Parameters
process_separately: A boolean (True / False) flag to indicate if source fields should be processed through the function chain separately or together.
                    The resulting output of the chain will always join the fields back together in a pandas series.

Example (False - This would produce an error for this example):
  placename:
    fields: [place_l, place_r]
    functions:
      - function: regex_sub
        pattern_from: "-"
        pattern_to: ""

Example (True):
  placename:
    fields: [place_l, place_r]
    process_separately: True
    functions:
      - function: regex_sub
        pattern_from: "-"
        pattern_to: ""

### Special Field Mapping Functions

#### copy_attribute_functions
Copies and appends the function chain from one or many target fields in the current target dataset.
copy_attribute_functions can exists within a function chain.

Example (generic):
  target_field:
    fields: source_field or [source_field] or [source_field, source_field, ...]
    functions:
      - function: copy_attribute_functions
        attributes: [target_field, ...]

##### Modifying Parameters
By default, copy_attribute_functions will copy the parameters of the copied functions. However, it is possible to modify any of these parameters.
- Only the required modifications need to be specified.
- Modifications can be specific to a target field or applied universally:
  - Specific: Applies the parameter modification only to the function from the specified target field's function chain.
  - Universal: Applies the parameter modification to all instances of that function from each copied function chain.

Example (generic - no modifications):
  target_field:
    fields: source_field or [source_field] or [source_field, source_field, ...]
    functions:
      - function: copy_attribute_functions
        attributes: [target_field, ...]

Example (generic - specific modifications):
  target_field:
    fields: source_field or [source_field] or [source_field, source_field, ...]
    functions:
      - function: copy_attribute_functions
        attributes:
          - target_field:
            function:
              parameter: ...
              parameter: ...
            function:
              ...
            ...
          - target_field:
          - ...

Example (generic - universal modifications):
  target_field:
    fields: source_field or [source_field] or [source_field, source_field, ...]
    functions:
      - function: copy_attribute_functions
        attributes: [target_field, ...]
        modify_parameters:
          function:
            parameter: ...
            parameter: ...
          function:
            ...
          ...

Example (specific modifications):
  namebody:
    fields: street_name
    functions:
      - function: copy_attribute_functions
        attributes:
          - strtypre:
            regex_find:
              strip_result: True
          - strtysuf

Example (universal modifications):
  namebody:
    fields: street_name
    functions:
      - function: copy_attribute_functions
        attributes: [strtypre, strtysuf]
        modify_parameters:
          regex_find:
            strip_result: True

#### split_record
Keeps one of the two source field values:
- First value if source fields' values are equal.
- Second value if source fields' values are unequal, after splitting the record into two records.
split_record can exists within a function chain (see section "Limitations/split_record").

Example (generic):
  target_field:
    fields: [source_field, source_field]
    functions:
      - function: split_record
        field: None

Example:
  placename:
    fields: [place_l, place_r]
    functions:
      - function: split_record
        field: None

#### Regular Expressions
Several field mapping functions take regular expressions as parameters, which are then analyzed using python's re package.
You can validate a regular expression using this resource: https://regex101.com/.

##### Keyword: domain
All regular expression parameters have been configured to accept the keyword: domain.
If detected, "domain" will be substituted with a list of domain values for the target field, joined by the regex "or" operator: "|".

Example:
  strdirpre:
    fields: dirprefix
    functions:
      - function: regex_find
        pattern: "\\b(domain)\\b"
        match_index: 0
        group_index: 0

  would be converted to:

  strdirpre:
    fields: dirprefix
    functions:
      - function: regex_find
        pattern: "\\b(East|Est|Nord|North|Ouest|South|Sud|West)\\b"
        match_index: 0
        group_index: 0

# Limitations

## Chaining
**Limitation 1:** All field mapping functions within a chain must accept the same amount of source fields.  
**Explanation 1:** When specifying multiple source fields in a field mapping, consider the following:
1. Source fields will always be compiled as a single pandas series (each record will be a list if multiple fields are given).
2. Most functions expect to receive a certain amount of input values for each record.

This results in the following erroneous scenarios:
1. Multiple fields being passed to a field mapping functions which only expects one source field.
2. Only one field being passed to a field mapping function which expects multiple source fields.

## split_record
**Limitation 1:** split_record must be the last function within a function chain.  
**Explanation 1:** The possibility of splitting records would create duplicate indexes. This would raise an error once the function chain finishes because the size of the target and source fields are no longer the same.

**Limitation 2:** split_record must only apply to a tabular dataset (pandas DataFrame).  
**Explanation 2:** split_record uses pandas.explode to split records on a field of nested values, however geopandas.explode will attempt to explode multipart geometries.
