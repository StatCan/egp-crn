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
  query: An attribute query using pandas.query syntax.

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
    query:

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
        pattern: "-"
        repl: ""

### Chaining
Multiple functions can be chained such that the output of one becomes the input of the next.

Example:
  l_hnumf:
    fields: left_civic_from
    functions:
      - function: regex_sub
        pattern: "-"
        repl: ""
      - function: regex_find
        pattern: "(^\\d+)"
        match_index: 0
        group_index: 0

#### Special Parameters

##### process_separately
process_separately: A boolean flag (default False) to indicate if source fields should be processed through the function chain separately or together.
                    The resulting output of the chain will always join the fields back together in a pandas series.

Example (False - This would produce an error for this example since the called function "regex_sub" only expects 1 field value):
  placename:
    fields: [place_l, place_r]
    functions:
      - function: regex_sub
        pattern: "-"
        repl: ""

Example (True):
  placename:
    fields: [place_l, place_r]
    process_separately: True
    functions:
      - function: regex_sub
        pattern: "-"
        repl: ""

##### iterate_cols
iterate_cols: A list of integers representing the indexes of columns within a nested Series which should be passed through the specified function.
              iterate_cols must be specified for each function where it is required. Indexes not listed will retain their original values.

Example: In this example, the directional prefix and suffix fields are given as codes, and must be run through the map_values function prior to concatenate. Nesting is required, but not all fields should be run through the first function.
  l_stname_c:
    fields: [SPN_Directional_Prefix, SPN_Street_Type_Prefix, SPN_Street_Name_Article, SPN_Street_Name_Body, SPN_Street_Type_Suffix, SPN_Directional_Suffix]
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

##### Regular Expressions
Several field mapping functions take regular expressions as parameters, which are then analyzed using python's re package.
You can validate a regular expression using this resource: https://regex101.com/.

##### Keyword: domain
All regular expression parameters have been configured to accept the keyword: (domain_{table}_{field}), where {table} and {field} are nrn dataset and field names.
If detected, "domain_{table}_{field}" will be substituted with a list of domain values for the specified table and field, joined by the regex "or" operator: "|". The brackets will be maintained.

Example:
  dirprefix:
    fields: strtypre
    functions:
      - function: regex_find
        pattern: "\\b(domain_strplaname_dirprefix)\\b"
        match_index: 0
        group_index: 0

  would be converted to:

  dirprefix:
    fields: strtypre
    functions:
      - function: regex_find
        pattern: "\\b(East|Est|Nord|North|Ouest|South|Sud|West)\\b"
        match_index: 0
        group_index: 0

# Limitations

## Nested Series
**Limitation 1:** Not all field mapping functions accept nested Series.
**Explanation 1:** If a nested Series is passed to a field mapping function which does not support it, an error will be raised.

# Missing table linkages.
Primary and foreign key fields which do not have any source field to map to will be set to the default field value. If this field must be populated, use "uuid" as the mapping field.
