-- Compile all NRN attributes into a single table.
SELECT REPLACE(blocked_passage.blocked_passage_id::text, '-', '') AS nid,
       REPLACE(blocked_passage.segment_id::text, '-', '') AS roadnid,
       blocked_passage_type_lookup.value_en AS blkpassty,
       acquisition_technique_lookup.value_en AS acqtech,
       blocked_passage.planimetric_accuracy AS accuracy,
       provider_lookup.value_en AS provider,
       blocked_passage.creation_date AS credate,
       blocked_passage.revision_date AS revdate,
       blocked_passage.geometry,
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

INNER JOIN public.blocked_passage blocked_passage ON nrn.segment_id = blocked_passage.segment_id

-- Join with lookup tables.
LEFT JOIN public.acquisition_technique_lookup acquisition_technique_lookup ON blocked_passage.acquisition_technique = acquisition_technique_lookup.code
LEFT JOIN public.provider_lookup provider_lookup ON blocked_passage.provider = provider_lookup.code
LEFT JOIN public.blocked_passage_type_lookup blocked_passage_type_lookup ON blocked_passage.blocked_passage_type = blocked_passage_type_lookup.code