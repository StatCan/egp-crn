-- Compile all NRN attributes into a single table.
SELECT REPLACE(toll_point.toll_point_id::text, '-', '') AS nid,
       REPLACE(toll_point.segment_id::text, '-', '') AS roadnid,
       toll_point_type_lookup.value_en AS tollpttype,
       acquisition_technique_lookup.value_en AS acqtech,
       toll_point.planimetric_accuracy AS accuracy,
       provider_lookup.value_en AS provider,
       toll_point.creation_date AS credate,
       toll_point.revision_date AS revdate,
       toll_point.geometry,
       {{ source_code }} AS datasetnam,
       {{ metacover }} AS metacover,
       {{ specvers }} AS specvers
FROM

  -- Subset records to the source province / territory.
  (SELECT segment_source.segment_id
   FROM
     (SELECT segment.segment_id,
             place_name_l.province AS l_province,
             place_name_r.province AS r_province
      FROM public.segment segment
      LEFT JOIN public.place_name place_name_l ON segment.segment_id_left = place_name_l.segment_id
      LEFT JOIN public.place_name place_name_r ON segment.segment_id_right = place_name_r.segment_id) segment_source
   WHERE segment_source.l_province = {{ source_code }} OR segment_source.r_province = {{ source_code }}) nrn

INNER JOIN public.toll_point toll_point ON nrn.segment_id = toll_point.segment_id

-- Join with lookup tables.
LEFT JOIN public.acquisition_technique_lookup acquisition_technique_lookup ON toll_point.acquisition_technique = acquisition_technique_lookup.code
LEFT JOIN public.provider_lookup provider_lookup ON toll_point.provider = provider_lookup.code
LEFT JOIN public.toll_point_type_lookup toll_point_type_lookup ON toll_point.toll_point_type = toll_point_type_lookup.code