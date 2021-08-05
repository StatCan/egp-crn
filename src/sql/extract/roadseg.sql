-- Drop / create sequences for incremental integer columns.
DROP SEQUENCE IF EXISTS roadseg_seq;
DROP SEQUENCE IF EXISTS ferryseg_seq;
CREATE TEMP SEQUENCE roadseg_seq;
CREATE TEMP SEQUENCE ferryseg_seq;

-- Create temporary tables (subqueries to be reused).

-- Create temporary table(s): route name.
WITH route_name_link AS
  (SELECT segment_id,
          route_name_en,
          route_name_fr,
          ROW_NUMBER() OVER (PARTITION BY segment_id)
   FROM public.route_name_link route_name_link_partition
   LEFT JOIN public.route_name route_name ON route_name_link_partition.route_name_id = route_name.route_name_id),
route_name_1 AS
  (SELECT *
   FROM route_name_link
   WHERE row_number = 1),
route_name_2 AS
  (SELECT *
   FROM route_name_link
   WHERE row_number = 2),
route_name_3 AS
  (SELECT *
   FROM route_name_link
   WHERE row_number = 3),
route_name_4 AS
  (SELECT *
   FROM route_name_link
   WHERE row_number = 4),

-- Create temporary table(s): route number.
route_number_link AS
  (SELECT segment_id,
          route_number,
          ROW_NUMBER() OVER (PARTITION BY segment_id)
   FROM public.route_number_link route_number_link_partition
   LEFT JOIN public.route_number route_number ON route_number_link_partition.route_number_id = route_number.route_number_id),
route_number_1 AS
  (SELECT *
   FROM route_number_link
   WHERE row_number = 1),
route_number_2 AS
  (SELECT *
   FROM route_number_link
   WHERE row_number = 2),
route_number_3 AS
  (SELECT *
   FROM route_number_link
   WHERE row_number = 3),
route_number_4 AS
  (SELECT *
   FROM route_number_link
   WHERE row_number = 4),
route_number_5 AS
  (SELECT *
   FROM route_number_link
   WHERE row_number = 5),

-- Create temporary table(s): street name.
street_name AS
  (SELECT *
   FROM
     (SELECT *
      FROM
        (SELECT *,
                ROW_NUMBER() OVER (PARTITION BY segment_id)
         FROM public.street_name_link) street_name_partition
      WHERE row_number = 1) street_name_link_filter
      LEFT JOIN public.street_name ON street_name_link_filter.street_name_id = public.street_name.street_name_id)

-- Create primary table.

-- Compile all NRN attributes into a single table.
SELECT REPLACE(nrn.segment_id::text, '-', '') AS segment_id,
       REPLACE(nrn.segment_id_left::text, '-', '') AS segment_id_left,
       REPLACE(nrn.segment_id_right::text, '-', '') AS segment_id_right,
       REPLACE(nrn.element_id::text, '-', '') AS nid,
       acquisition_technique_lookup.value_en AS acqtech,
       nrn.planimetric_accuracy AS accuracy,
       provider_lookup.value_en AS provider,
       nrn.creation_date AS credate,
       nrn.revision_date AS revdate,
       nrn.segment_type,
       nrn.geometry,
       strplaname_l_acquisition_technique_lookup.value_en AS strplaname_l_acqtech,
       strplaname_l_provider_lookup.value_en AS strplaname_l_provider,
       nrn.strplaname_l_creation_date AS strplaname_l_credate,
       nrn.strplaname_l_revision_date AS strplaname_l_revdate,
       nrn.strplaname_l_place_name AS strplaname_l_placename,
       strplaname_l_place_type_lookup.value_en AS strplaname_l_placetype,
       strplaname_l_province_lookup.value_en AS strplaname_l_province,
       strplaname_r_acquisition_technique_lookup.value_en AS strplaname_r_acqtech,
       strplaname_r_provider_lookup.value_en AS strplaname_r_provider,
       nrn.strplaname_r_creation_date AS strplaname_r_credate,
       nrn.strplaname_r_revision_date AS strplaname_r_revdate,
       nrn.strplaname_r_place_name AS strplaname_r_placename,
       strplaname_r_place_type_lookup.value_en AS strplaname_r_placetype,
       strplaname_r_province_lookup.value_en AS strplaname_r_province,
       closing_period_lookup.value_en AS closing,
       exit_number.exit_number AS exitnbr,
       functional_road_class_lookup.value_en AS roadclass,
       CASE road_surface_type_lookup.value_en
         WHEN 'Unknown' THEN 'Unknown'
         WHEN 'Rigid' THEN 'Paved'
         WHEN 'Flexible' THEN 'Paved'
         WHEN 'Blocks' THEN 'Paved'
         WHEN 'Gravel' THEN 'Unpaved'
         WHEN 'Dirt' THEN 'Unpaved'
         WHEN 'Paved unknown' THEN 'Paved'
         WHEN 'Unpaved unknown' THEN 'Unpaved'
       END pavstatus,
       CASE road_surface_type_lookup.value_en
         WHEN 'Unknown' THEN 'Unknown'
         WHEN 'Rigid' THEN 'Rigid'
         WHEN 'Flexible' THEN 'Flexible'
         WHEN 'Blocks' THEN 'Blocks'
         WHEN 'Gravel' THEN 'None'
         WHEN 'Dirt' THEN 'None'
         WHEN 'Paved unknown' THEN 'Unknown'
         WHEN 'Unpaved unknown' THEN 'None'
       END pavsurf,
       CASE road_surface_type_lookup.value_en
         WHEN 'Unknown' THEN 'Unknown'
         WHEN 'Rigid' THEN 'None'
         WHEN 'Flexible' THEN 'None'
         WHEN 'Blocks' THEN 'None'
         WHEN 'Gravel' THEN 'Gravel'
         WHEN 'Dirt' THEN 'Dirt'
         WHEN 'Paved unknown' THEN 'None'
         WHEN 'Unpaved unknown' THEN 'Unknown'
       END unpavsurf,
       CASE
         WHEN structure_source.structure_type = 0 THEN 'None'
         ELSE REPLACE(structure_source.structure_id::text, '-', '')
       END structid,
       structure_type_lookup.value_en AS structtype,
       structure_source.structure_name_en AS strunameen,
       structure_source.structure_name_fr AS strunamefr,
       traffic_direction_lookup.value_en AS trafficdir,
       addrange_l.first_house_number AS addrange_l_hnumf,
       addrange_l.first_house_number_suffix AS addrange_l_hnumsuff,
       addrange_l_first_house_number_type_lookup.value_en AS addrange_l_hnumtypf,
       addrange_l.last_house_number AS addrange_l_hnuml,
       addrange_l.last_house_number_suffix AS addrange_l_hnumsufl,
       addrange_l_last_house_number_type_lookup.value_en AS addrange_l_hnumtypl,
       addrange_l_house_number_structure_lookup.value_en AS addrange_l_hnumstr,
       addrange_l_reference_system_indicator_lookup.value_en AS addrange_l_rfsysind,
       REPLACE(COALESCE(addrange_r.address_range_id, nrn.segment_id)::text, '-', '') AS addrange_nid,
       addrange_r_acquisition_technique_lookup.value_en AS addrange_acqtech,
       addrange_r_provider_lookup.value_en AS addrange_provider,
       addrange_r.creation_date AS addrange_credate,
       addrange_r.revision_date AS addrange_revdate,
       addrange_r.first_house_number AS addrange_r_hnumf,
       addrange_r.first_house_number_suffix AS addrange_r_hnumsuff,
       addrange_r_first_house_number_type_lookup.value_en AS addrange_r_hnumtypf,
       addrange_r.last_house_number AS addrange_r_hnuml,
       addrange_r.last_house_number_suffix AS addrange_r_hnumsufl,
       addrange_r_last_house_number_type_lookup.value_en AS addrange_r_hnumtypl,
       addrange_r_house_number_structure_lookup.value_en AS addrange_r_hnumstr,
       addrange_r_reference_system_indicator_lookup.value_en AS addrange_r_rfsysind,
       number_of_lanes.number_of_lanes AS nbrlanes,
       road_jurisdiction.road_jurisdiction AS roadjuris,
       route_name_1.route_name_en AS rtename1en,
       route_name_1.route_name_fr AS rtename1fr,
       route_name_2.route_name_en AS rtename2en,
       route_name_2.route_name_fr AS rtename2fr,
       route_name_3.route_name_en AS rtename3en,
       route_name_3.route_name_fr AS rtename3fr,
       route_name_4.route_name_en AS rtename4en,
       route_name_4.route_name_fr AS rtename4fr,
       route_number_1.route_number AS rtnumber1,
       route_number_2.route_number AS rtnumber2,
       route_number_3.route_number AS rtnumber3,
       route_number_4.route_number AS rtnumber4,
       route_number_5.route_number AS rtnumber5,
       speed.speed AS speed,
       street_name_l.street_name_concatenated AS l_stname_c,
       strplaname_l_street_direction_prefix_lookup.value_en AS strplaname_l_dirprefix,
       strplaname_l_street_type_prefix_lookup.value_en AS strplaname_l_strtypre,
       strplaname_l_street_article_lookup.value_en AS strplaname_l_starticle,
       street_name_l.street_name_body AS strplaname_l_namebody,
       strplaname_l_street_type_suffix_lookup.value_en AS strplaname_l_strtysuf,
       strplaname_l_street_direction_suffix_lookup.value_en AS strplaname_l_dirsuffix,
       street_name_r.street_name_concatenated AS r_stname_c,
       strplaname_r_street_direction_prefix_lookup.value_en AS strplaname_r_dirprefix,
       strplaname_r_street_type_prefix_lookup.value_en AS strplaname_r_strtypre,
       strplaname_r_street_article_lookup.value_en AS strplaname_r_starticle,
       street_name_r.street_name_body AS strplaname_r_namebody,
       strplaname_r_street_type_suffix_lookup.value_en AS strplaname_r_strtysuf,
       strplaname_r_street_direction_suffix_lookup.value_en AS strplaname_r_dirsuffix,
       CASE nrn.segment_type
         WHEN 1 THEN nextval('roadseg_seq')
         ELSE -1
       END roadsegid,
       CASE nrn.segment_type
         WHEN 2 THEN nextval('ferryseg_seq')
         ELSE -1
       END ferrysegid,
       {{ source_code }} AS datasetnam,
       {{ metacover }} AS metacover,
       {{ specvers }} AS specvers,
       {{ muniquad }} AS muniquad,
       'None' AS l_altnanid,
       'None' AS r_altnanid,
       CASE
         WHEN addrange_l.first_house_number IS NULL
         OR addrange_l.first_house_number IN (-1, 0)
         OR addrange_l.last_house_number IS NULL
         OR addrange_l.last_house_number IN (-1, 0)
         OR addrange_l.first_house_number = addrange_l.last_house_number THEN 'Not Applicable'
         WHEN addrange_l.first_house_number < addrange_l.last_house_number THEN 'Same Direction'
         ELSE 'Opposite Direction'
       END addrange_l_digdirfg,
       CASE
         WHEN addrange_r.first_house_number IS NULL
         OR addrange_r.first_house_number IN (-1, 0)
         OR addrange_r.last_house_number IS NULL
         OR addrange_r.last_house_number IN (-1, 0)
         OR addrange_r.first_house_number = addrange_r.last_house_number THEN 'Not Applicable'
         WHEN addrange_r.first_house_number < addrange_r.last_house_number THEN 'Same Direction'
         ELSE 'Opposite Direction'
       END addrange_r_digdirfg
FROM

  -- Subset records to the source province / territory.
  (SELECT *
   FROM
     (SELECT segment.*,
             place_name_l.acquisition_technique AS strplaname_l_acquisition_technique,
             place_name_l.provider AS strplaname_l_provider,
             place_name_l.creation_date AS strplaname_l_creation_date,
             place_name_l.revision_date AS strplaname_l_revision_date,
             place_name_l.place_name AS strplaname_l_place_name,
             place_name_l.place_type AS strplaname_l_place_type,
             place_name_l.province AS strplaname_l_province,
             place_name_r.acquisition_technique AS strplaname_r_acquisition_technique,
             place_name_r.provider AS strplaname_r_provider,
             place_name_r.creation_date AS strplaname_r_creation_date,
             place_name_r.revision_date AS strplaname_r_revision_date,
             place_name_r.place_name AS strplaname_r_place_name,
             place_name_r.place_type AS strplaname_r_place_type,
             place_name_r.province AS strplaname_r_province
      FROM public.segment segment
      LEFT JOIN public.place_name place_name_l ON segment.segment_id_left = place_name_l.segment_id
      LEFT JOIN public.place_name place_name_r ON segment.segment_id_right = place_name_r.segment_id) segment_source
   WHERE segment_source.strplaname_l_province = {{ source_code }} OR segment_source.strplaname_r_province = {{ source_code }}) nrn

-- Join with all linked datasets.
LEFT JOIN public.closing_period closing_period ON nrn.segment_id = closing_period.segment_id
LEFT JOIN public.exit_number exit_number ON nrn.segment_id = exit_number.segment_id
LEFT JOIN public.functional_road_class functional_road_class ON nrn.segment_id = functional_road_class.segment_id
LEFT JOIN public.road_surface_type road_surface_type ON nrn.segment_id = road_surface_type.segment_id

LEFT JOIN
  (SELECT structure_link.segment_id,
          structure.*
   FROM public.structure_link
   LEFT JOIN public.structure structure ON structure_link.structure_id = structure.structure_id) structure_source
ON nrn.segment_id = structure_source.segment_id

LEFT JOIN public.traffic_direction traffic_direction ON nrn.segment_id = traffic_direction.segment_id
LEFT JOIN public.address_range addrange_l ON nrn.segment_id_left = addrange_l.segment_id
LEFT JOIN public.address_range addrange_r ON nrn.segment_id_right = addrange_r.segment_id
LEFT JOIN public.number_of_lanes number_of_lanes ON nrn.segment_id_right = number_of_lanes.segment_id
LEFT JOIN public.road_jurisdiction road_jurisdiction ON nrn.segment_id_right = road_jurisdiction.segment_id
LEFT JOIN route_name_1 ON nrn.segment_id_right = route_name_1.segment_id
LEFT JOIN route_name_2 ON nrn.segment_id_right = route_name_2.segment_id
LEFT JOIN route_name_3 ON nrn.segment_id_right = route_name_3.segment_id
LEFT JOIN route_name_4 ON nrn.segment_id_right = route_name_4.segment_id
LEFT JOIN route_number_1 ON nrn.segment_id_right = route_number_1.segment_id
LEFT JOIN route_number_2 ON nrn.segment_id_right = route_number_2.segment_id
LEFT JOIN route_number_3 ON nrn.segment_id_right = route_number_3.segment_id
LEFT JOIN route_number_4 ON nrn.segment_id_right = route_number_4.segment_id
LEFT JOIN route_number_5 ON nrn.segment_id_right = route_number_5.segment_id
LEFT JOIN public.speed speed ON nrn.segment_id_right = speed.segment_id
LEFT JOIN street_name street_name_l ON nrn.segment_id_left = street_name_l.segment_id
LEFT JOIN street_name street_name_r ON nrn.segment_id_right = street_name_r.segment_id

-- Join with lookup tables.
LEFT JOIN public.acquisition_technique_lookup acquisition_technique_lookup ON nrn.acquisition_technique = acquisition_technique_lookup.code
LEFT JOIN public.provider_lookup provider_lookup ON nrn.provider = provider_lookup.code
LEFT JOIN public.acquisition_technique_lookup strplaname_l_acquisition_technique_lookup ON nrn.strplaname_l_acquisition_technique = strplaname_l_acquisition_technique_lookup.code
LEFT JOIN public.provider_lookup strplaname_l_provider_lookup ON nrn.strplaname_l_provider = strplaname_l_provider_lookup.code
LEFT JOIN public.place_type_lookup strplaname_l_place_type_lookup ON nrn.strplaname_l_place_type = strplaname_l_place_type_lookup.code
LEFT JOIN public.province_lookup strplaname_l_province_lookup ON nrn.strplaname_l_province = strplaname_l_province_lookup.code
LEFT JOIN public.acquisition_technique_lookup strplaname_r_acquisition_technique_lookup ON nrn.strplaname_r_acquisition_technique = strplaname_r_acquisition_technique_lookup.code
LEFT JOIN public.provider_lookup strplaname_r_provider_lookup ON nrn.strplaname_r_provider = strplaname_r_provider_lookup.code
LEFT JOIN public.place_type_lookup strplaname_r_place_type_lookup ON nrn.strplaname_r_place_type = strplaname_r_place_type_lookup.code
LEFT JOIN public.province_lookup strplaname_r_province_lookup ON nrn.strplaname_r_province = strplaname_r_province_lookup.code
LEFT JOIN public.closing_period_lookup closing_period_lookup ON closing_period.closing_period = closing_period_lookup.code
LEFT JOIN public.functional_road_class_lookup functional_road_class_lookup ON functional_road_class.functional_road_class = functional_road_class_lookup.code
LEFT JOIN public.road_surface_type_lookup road_surface_type_lookup ON road_surface_type.road_surface_type = road_surface_type_lookup.code
LEFT JOIN public.structure_type_lookup structure_type_lookup ON structure_source.structure_type = structure_type_lookup.code
LEFT JOIN public.traffic_direction_lookup traffic_direction_lookup ON traffic_direction.traffic_direction = traffic_direction_lookup.code
LEFT JOIN public.house_number_type_lookup addrange_l_first_house_number_type_lookup ON addrange_l.first_house_number_type = addrange_l_first_house_number_type_lookup.code
LEFT JOIN public.house_number_type_lookup addrange_l_last_house_number_type_lookup ON addrange_l.last_house_number_type = addrange_l_last_house_number_type_lookup.code
LEFT JOIN public.house_number_structure_lookup addrange_l_house_number_structure_lookup ON addrange_l.house_number_structure = addrange_l_house_number_structure_lookup.code
LEFT JOIN public.reference_system_indicator_lookup addrange_l_reference_system_indicator_lookup ON addrange_l.reference_system_indicator = addrange_l_reference_system_indicator_lookup.code
LEFT JOIN public.acquisition_technique_lookup addrange_r_acquisition_technique_lookup ON addrange_r.acquisition_technique = addrange_r_acquisition_technique_lookup.code
LEFT JOIN public.provider_lookup addrange_r_provider_lookup ON addrange_r.provider = addrange_r_provider_lookup.code
LEFT JOIN public.house_number_type_lookup addrange_r_first_house_number_type_lookup ON addrange_r.first_house_number_type = addrange_r_first_house_number_type_lookup.code
LEFT JOIN public.house_number_type_lookup addrange_r_last_house_number_type_lookup ON addrange_r.last_house_number_type = addrange_r_last_house_number_type_lookup.code
LEFT JOIN public.house_number_structure_lookup addrange_r_house_number_structure_lookup ON addrange_r.house_number_structure = addrange_r_house_number_structure_lookup.code
LEFT JOIN public.reference_system_indicator_lookup addrange_r_reference_system_indicator_lookup ON addrange_r.reference_system_indicator = addrange_r_reference_system_indicator_lookup.code
LEFT JOIN public.street_direction_lookup strplaname_l_street_direction_prefix_lookup ON street_name_l.street_direction_prefix = strplaname_l_street_direction_prefix_lookup.code
LEFT JOIN public.street_type_lookup strplaname_l_street_type_prefix_lookup ON street_name_l.street_type_prefix = strplaname_l_street_type_prefix_lookup.code
LEFT JOIN public.street_article_lookup strplaname_l_street_article_lookup ON street_name_l.street_article = strplaname_l_street_article_lookup.code
LEFT JOIN public.street_type_lookup strplaname_l_street_type_suffix_lookup ON street_name_l.street_type_suffix = strplaname_l_street_type_suffix_lookup.code
LEFT JOIN public.street_direction_lookup strplaname_l_street_direction_suffix_lookup ON street_name_l.street_direction_suffix = strplaname_l_street_direction_suffix_lookup.code
LEFT JOIN public.street_direction_lookup strplaname_r_street_direction_prefix_lookup ON street_name_r.street_direction_prefix = strplaname_r_street_direction_prefix_lookup.code
LEFT JOIN public.street_type_lookup strplaname_r_street_type_prefix_lookup ON street_name_r.street_type_prefix = strplaname_r_street_type_prefix_lookup.code
LEFT JOIN public.street_article_lookup strplaname_r_street_article_lookup ON street_name_r.street_article = strplaname_r_street_article_lookup.code
LEFT JOIN public.street_type_lookup strplaname_r_street_type_suffix_lookup ON street_name_r.street_type_suffix = strplaname_r_street_type_suffix_lookup.code
LEFT JOIN public.street_direction_lookup strplaname_r_street_direction_suffix_lookup ON street_name_r.street_direction_suffix = strplaname_r_street_direction_suffix_lookup.code