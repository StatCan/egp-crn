SELECT blocked_passage_source.blocked_passage_id AS nid,
       blocked_passage_source.segment_id AS roadnid,
       blocked_passage_source.blocked_passage_type AS blkpassty,
       blocked_passage_source.acquisition_technique AS acqtech,
       blocked_passage_source.planimetric_accuracy AS accuracy,
       blocked_passage_source.provider,
       blocked_passage_source.creation_date AS credate,
       blocked_passage_source.revision_date AS revdate,
       blocked_passage_source.geometry
FROM
  (SELECT blocked_passage.*
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
   LEFT JOIN public.blocked_passage blocked_passage ON nrn.segment_id = blocked_passage.segment_id) blocked_passage_source
WHERE blocked_passage_source.blocked_passage_id IS NOT NULL