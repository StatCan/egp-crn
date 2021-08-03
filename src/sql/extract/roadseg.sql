-- Create temporary tables (subqueries to be reused).

-- Create temporary table(s): route name.
WITH route_name_link AS
  (SELECT route_name_link_full.segment_id,
          route_name_link_full.route_name_en,
          route_name_link_full.route_name_fr,
          route_name_link_full.row_number
   FROM
     (SELECT *,
             ROW_NUMBER() OVER (PARTITION BY segment_id)
      FROM public.route_name_link route_name_link_partition
      LEFT JOIN public.route_name route_name ON route_name_link_partition.route_name_id = route_name.route_name_id) route_name_link_full),
route_name_1 AS
  (SELECT segment_id,
          route_name_en AS rtename1en,
          route_name_fr AS rtename1fr
   FROM route_name_link
   WHERE row_number = 1),
route_name_2 AS
  (SELECT segment_id,
          route_name_en AS rtename2en,
          route_name_fr AS rtename2fr
   FROM route_name_link
   WHERE row_number = 2),
route_name_3 AS
  (SELECT segment_id,
          route_name_en AS rtename3en,
          route_name_fr AS rtename3fr
   FROM route_name_link
   WHERE row_number = 3),
route_name_4 AS
  (SELECT segment_id,
          route_name_en AS rtename4en,
          route_name_fr AS rtename4fr
   FROM route_name_link
   WHERE row_number = 4),

-- Create temporary table(s): route number.
route_number_link AS
  (SELECT route_number_link_full.segment_id,
          route_number_link_full.route_number,
          route_number_link_full.route_number_alpha,
          route_number_link_full.row_number
   FROM
     (SELECT *,
             ROW_NUMBER() OVER (PARTITION BY segment_id)
      FROM public.route_number_link route_number_link_partition
      LEFT JOIN public.route_number route_number ON route_number_link_partition.route_number_id = route_number.route_number_id) route_number_link_full),
route_number_1 AS
  (SELECT segment_id,
          route_number AS rtnumber1,
          route_number_alpha AS rtnumber1_alpha
   FROM route_number_link
   WHERE row_number = 1),
route_number_2 AS
  (SELECT segment_id,
          route_number AS rtnumber2,
          route_number_alpha AS rtnumber2_alpha
   FROM route_number_link
   WHERE row_number = 2),
route_number_3 AS
  (SELECT segment_id,
          route_number AS rtnumber3,
          route_number_alpha AS rtnumber3_alpha
   FROM route_number_link
   WHERE row_number = 3),
route_number_4 AS
  (SELECT segment_id,
          route_number AS rtnumber4,
          route_number_alpha AS rtnumber4_alpha
   FROM route_number_link
   WHERE row_number = 4),
route_number_5 AS
  (SELECT segment_id,
          route_number AS rtnumber5,
          route_number_alpha AS rtnumber5_alpha
   FROM route_number_link
   WHERE row_number = 5),

-- Create temporary table(s): street name.
street_name AS
  (SELECT street_name_link_full.segment_id,
          street_name_link_full.street_name_concatenated AS stname_c,
          street_name_link_full.street_direction_prefix AS dirprefix,
          street_name_link_full.street_type_prefix AS strtypre,
          street_name_link_full.street_article AS starticle,
          street_name_link_full.street_name_body AS namebody,
          street_name_link_full.street_type_suffix AS strtysuf,
          street_name_link_full.street_direction_suffix AS dirsuffix
   FROM
     (SELECT *
      FROM
        (SELECT *
         FROM
           (SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY segment_id)
            FROM public.street_name_link) street_name_partition
         WHERE row_number = 1) street_name_link_filter
         LEFT JOIN public.street_name ON street_name_link_filter.street_name_id = public.street_name.street_name_id) street_name_link_full)

-- Compile all NRN attributes into a single table.
SELECT nrn.segment_id,
       nrn.segment_id_left,
       nrn.segment_id_right,
       nrn.element_id AS nid,
       acquisition_technique_lookup.value_en AS acqtech,
       nrn.planimetric_accuracy AS accuracy,
       provider_lookup.value_en AS provider,
       nrn.creation_date AS credate,
       nrn.revision_date AS revdate,
       nrn.segment_type,
       nrn.geometry,
       strplaname_l_acquisition_technique_lookup.value_en AS strplaname_l_acqtech,
       strplaname_l_provider_lookup.value_en AS strplaname_l_provider,
       nrn.strplaname_l_credate,
       nrn.strplaname_l_revdate,
       nrn.strplaname_l_placename,
       strplaname_l_place_type_lookup.value_en AS strplaname_l_placetype,
       strplaname_l_province_lookup.value_en AS strplaname_l_province,
       strplaname_r_acquisition_technique_lookup.value_en AS strplaname_r_acqtech,
       strplaname_r_provider_lookup.value_en AS strplaname_r_provider,
       nrn.strplaname_r_credate,
       nrn.strplaname_r_revdate,
       nrn.strplaname_r_placename,
       strplaname_r_place_type_lookup.value_en AS strplaname_r_placetype,
       strplaname_r_province_lookup.value_en AS strplaname_r_province,
       closing_period_lookup.value_en AS closing,
       exit_number.exit_number AS exitnbr,
       exit_number.exit_number_alpha AS exitnbr_alpha,
       functional_road_class_lookup.value_en AS roadclass,
       road_surface_type_lookup.value_en AS road_surface_type,
       structure_source.structure_id AS structid,
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
       addrange_r.address_range_id AS addrange_nid,
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
       route_name_1.rtename1en,
       route_name_1.rtename1fr,
       route_name_2.rtename2en,
       route_name_2.rtename2fr,
       route_name_3.rtename3en,
       route_name_3.rtename3fr,
       route_name_4.rtename4en,
       route_name_4.rtename4fr,
       route_number_1.rtnumber1,
       route_number_1.rtnumber1_alpha,
       route_number_2.rtnumber2,
       route_number_2.rtnumber2_alpha,
       route_number_3.rtnumber3,
       route_number_3.rtnumber3_alpha,
       route_number_4.rtnumber4,
       route_number_4.rtnumber4_alpha,
       route_number_5.rtnumber5,
       route_number_5.rtnumber5_alpha,
       speed.speed AS speed,
       street_name_l.stname_c AS l_stname_c,
       strplaname_l_street_direction_prefix_lookup.value_en AS strplaname_l_dirprefix,
       strplaname_l_street_type_prefix_lookup.value_en AS strplaname_l_strtypre,
       strplaname_l_street_article_lookup.value_en AS strplaname_l_starticle,
       street_name_l.namebody AS strplaname_l_namebody,
       strplaname_l_street_type_suffix_lookup.value_en AS strplaname_l_strtysuf,
       strplaname_l_street_direction_suffix_lookup.value_en AS strplaname_l_dirsuffix,
       street_name_r.stname_c AS r_stname_c,
       strplaname_r_street_direction_prefix_lookup.value_en AS strplaname_r_dirprefix,
       strplaname_r_street_type_prefix_lookup.value_en AS strplaname_r_strtypre,
       strplaname_r_street_article_lookup.value_en AS strplaname_r_starticle,
       street_name_r.namebody AS strplaname_r_namebody,
       strplaname_r_street_type_suffix_lookup.value_en AS strplaname_r_strtysuf,
       strplaname_r_street_direction_suffix_lookup.value_en AS strplaname_r_dirsuffix
FROM

  -- Subset records to the source province / territory.
  (SELECT *
   FROM
     (SELECT segment.*,
             place_name_l.acquisition_technique AS strplaname_l_acqtech,
             place_name_l.provider AS strplaname_l_provider,
             place_name_l.creation_date AS strplaname_l_credate,
             place_name_l.revision_date AS strplaname_l_revdate,
             place_name_l.place_name AS strplaname_l_placename,
             place_name_l.place_type AS strplaname_l_placetype,
             place_name_l.province AS strplaname_l_province,
             place_name_r.acquisition_technique AS strplaname_r_acqtech,
             place_name_r.provider AS strplaname_r_provider,
             place_name_r.creation_date AS strplaname_r_credate,
             place_name_r.revision_date AS strplaname_r_revdate,
             place_name_r.place_name AS strplaname_r_placename,
             place_name_r.place_type AS strplaname_r_placetype,
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
          structure_link.structure_id,
          structure.structure_type,
          structure.structure_name_en,
          structure.structure_name_fr
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
LEFT JOIN public.acquisition_technique_lookup strplaname_l_acquisition_technique_lookup ON nrn.strplaname_l_acqtech = strplaname_l_acquisition_technique_lookup.code
LEFT JOIN public.provider_lookup strplaname_l_provider_lookup ON nrn.strplaname_l_provider = strplaname_l_provider_lookup.code
LEFT JOIN public.place_type_lookup strplaname_l_place_type_lookup ON nrn.strplaname_l_placetype = strplaname_l_place_type_lookup.code
LEFT JOIN public.province_lookup strplaname_l_province_lookup ON nrn.strplaname_l_province = strplaname_l_province_lookup.code
LEFT JOIN public.acquisition_technique_lookup strplaname_r_acquisition_technique_lookup ON nrn.strplaname_r_acqtech = strplaname_r_acquisition_technique_lookup.code
LEFT JOIN public.provider_lookup strplaname_r_provider_lookup ON nrn.strplaname_r_provider = strplaname_r_provider_lookup.code
LEFT JOIN public.place_type_lookup strplaname_r_place_type_lookup ON nrn.strplaname_r_placetype = strplaname_r_place_type_lookup.code
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
LEFT JOIN public.street_direction_lookup strplaname_l_street_direction_prefix_lookup ON street_name_l.dirprefix = strplaname_l_street_direction_prefix_lookup.code
LEFT JOIN public.street_type_lookup strplaname_l_street_type_prefix_lookup ON street_name_l.strtypre = strplaname_l_street_type_prefix_lookup.code
LEFT JOIN public.street_article_lookup strplaname_l_street_article_lookup ON street_name_l.starticle = strplaname_l_street_article_lookup.code
LEFT JOIN public.street_type_lookup strplaname_l_street_type_suffix_lookup ON street_name_l.strtysuf = strplaname_l_street_type_suffix_lookup.code
LEFT JOIN public.street_direction_lookup strplaname_l_street_direction_suffix_lookup ON street_name_l.dirsuffix = strplaname_l_street_direction_suffix_lookup.code
LEFT JOIN public.street_direction_lookup strplaname_r_street_direction_prefix_lookup ON street_name_r.dirprefix = strplaname_r_street_direction_prefix_lookup.code
LEFT JOIN public.street_type_lookup strplaname_r_street_type_prefix_lookup ON street_name_r.strtypre = strplaname_r_street_type_prefix_lookup.code
LEFT JOIN public.street_article_lookup strplaname_r_street_article_lookup ON street_name_r.starticle = strplaname_r_street_article_lookup.code
LEFT JOIN public.street_type_lookup strplaname_r_street_type_suffix_lookup ON street_name_r.strtysuf = strplaname_r_street_type_suffix_lookup.code
LEFT JOIN public.street_direction_lookup strplaname_r_street_direction_suffix_lookup ON street_name_r.dirsuffix = strplaname_r_street_direction_suffix_lookup.code