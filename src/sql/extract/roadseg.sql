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
SELECT nrn.*,
       closing_period.closing_period AS closing,
       exit_number.exit_number AS exitnbr,
       exit_number.exit_number_alpha AS exitnbr_alpha,
       functional_road_class.functional_road_class AS roadclass,
       road_surface_type.road_surface_type AS road_surface_type,
       structure_source.structid,
       structure_source.structtype,
       structure_source.strunameen,
       structure_source.strunamefr,
       traffic_direction.traffic_direction AS trafficdir,
       address_range_l.first_house_number AS addrange_l_hnumf,
       address_range_l.first_house_number_suffix AS addrange_l_hnumsuff,
       address_range_l.first_house_number_type AS addrange_l_hnumtypf,
       address_range_l.last_house_number AS addrange_l_hnuml,
       address_range_l.last_house_number_suffix AS addrange_l_hnumsufl,
       address_range_l.last_house_number_type AS addrange_l_hnumtypl,
       address_range_l.house_number_structure AS addrange_l_hnumstr,
       address_range_l.reference_system_indicator AS addrange_l_rfsysind,
       address_range_r.address_range_id AS addrange_nid,
       address_range_r.acquisition_technique AS addrange_acqtech,
       address_range_r.provider AS addrange_provider,
       address_range_r.creation_date AS addrange_credate,
       address_range_r.revision_date AS addrange_revdate,
       address_range_r.first_house_number AS addrange_r_hnumf,
       address_range_r.first_house_number_suffix AS addrange_r_hnumsuff,
       address_range_r.first_house_number_type AS addrange_r_hnumtypf,
       address_range_r.last_house_number AS addrange_r_hnuml,
       address_range_r.last_house_number_suffix AS addrange_r_hnumsufl,
       address_range_r.last_house_number_type AS addrange_r_hnumtypl,
       address_range_r.house_number_structure AS addrange_r_hnumstr,
       address_range_r.reference_system_indicator AS addrange_r_rfsysind,
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
       street_name_l.dirprefix AS strplaname_l_dirprefix,
       street_name_l.strtypre AS strplaname_l_strtypre,
       street_name_l.starticle AS strplaname_l_starticle,
       street_name_l.namebody AS strplaname_l_namebody,
       street_name_l.strtysuf AS strplaname_l_strtysuf,
       street_name_l.dirsuffix AS strplaname_l_dirsuffix,
       street_name_r.stname_c AS r_stname_c,
       street_name_r.dirprefix AS strplaname_r_dirprefix,
       street_name_r.strtypre AS strplaname_r_strtypre,
       street_name_r.starticle AS strplaname_r_starticle,
       street_name_r.namebody AS strplaname_r_namebody,
       street_name_r.strtysuf AS strplaname_r_strtysuf,
       street_name_r.dirsuffix AS strplaname_r_dirsuffix
FROM

  -- Subset segments to the source province / territory.
  (SELECT *
   FROM
     (SELECT segment.segment_id,
             segment.segment_id_left,
             segment.segment_id_right,
             segment.element_id AS nid,
             segment.acquisition_technique AS acqtech,
             segment.planimetric_accuracy AS accuracy,
             segment.provider,
             segment.creation_date AS credate,
             segment.revision_date AS revdate,
             segment.segment_type,
             segment.geometry,
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
          structure_link.structure_id AS structid,
          structure.structure_type AS structtype,
          structure.structure_name_en AS strunameen,
          structure.structure_name_fr AS strunamefr
   FROM public.structure_link
   LEFT JOIN public.structure structure ON structure_link.structure_id = structure.structure_id) structure_source
ON nrn.segment_id = structure_source.segment_id

LEFT JOIN public.traffic_direction traffic_direction ON nrn.segment_id = traffic_direction.segment_id
LEFT JOIN public.address_range address_range_l ON nrn.segment_id_left = address_range_l.segment_id
LEFT JOIN public.address_range address_range_r ON nrn.segment_id_right = address_range_r.segment_id
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