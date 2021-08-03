-- Compile all NRN attributes into a single table.
SELECT junction_source.junction_id AS nid,
       acquisition_technique_lookup.value_en AS acqtech,
       junction_source.planimetric_accuracy AS accuracy,
       provider_lookup.value_en AS provider,
       junction_source.creation_date AS credate,
       junction_source.revision_date AS revdate,
       junction_source.exit_number AS exitnbr,
       junction_type_lookup.value_en AS junctype,
       junction_source.geometry
FROM

  -- Subset records to the source province / territory.
  (SELECT junction.*
   FROM public.junction junction
   WHERE junction.province = {{ source_code }}) junction_source

-- Join with lookup tables.
LEFT JOIN public.acquisition_technique_lookup acquisition_technique_lookup ON junction_source.acquisition_technique = acquisition_technique_lookup.code
LEFT JOIN public.provider_lookup provider_lookup ON junction_source.provider = provider_lookup.code
LEFT JOIN public.junction_type_lookup junction_type_lookup ON junction_source.junction_type = junction_type_lookup.code