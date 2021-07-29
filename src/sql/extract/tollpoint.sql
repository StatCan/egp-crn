SELECT toll_point_source.toll_point_id AS nid,
       toll_point_source.segment_id AS roadnid,
       toll_point_source.toll_point_type AS tollpttype,
       toll_point_source.acquisition_technique AS acqtech,
       toll_point_source.planimetric_accuracy AS accuracy,
       toll_point_source.provider,
       toll_point_source.creation_date AS credate,
       toll_point_source.revision_date AS revdate,
       toll_point_source.geometry
FROM
  (SELECT toll_point.*
   FROM
     (SELECT segment_source.segment_id
      FROM
        (SELECT segment.segment_id,
                place_name_l.province AS l_province,
                place_name_r.province AS r_province
         FROM public.segment segment
         LEFT JOIN public.place_name place_name_l ON segment.segment_id_left = place_name_l.segment_id
         LEFT JOIN public.place_name place_name_r ON segment.segment_id_right = place_name_r.segment_id) segment_source
      WHERE segment_source.l_province = {{ source_code }} OR segment_source.r_province = {{ source_code }}) nrn
   LEFT JOIN public.toll_point toll_point ON nrn.segment_id = toll_point.segment_id) toll_point_source
WHERE toll_point_source.toll_point_id IS NOT NULL