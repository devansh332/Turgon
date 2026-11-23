SELECT
  -- ticket_id: preserve raw trimmed value (keep as business key); never convert common placeholders to NULL here
  COALESCE(NULLIF(trim(bronze.ticket_id), ''), '') AS ticket_id,

  -- submitted_by: simple trim, empty/NULL placeholders -> NULL
  (CASE
     WHEN trim(bronze.submitted_by) = '' THEN NULL
     WHEN lower(trim(bronze.submitted_by)) = 'null' THEN NULL
     ELSE trim(bronze.submitted_by)
   END) AS submitted_by,

  -- assigned_to: trim, placeholders -> NULL, basic shape guard
  (CASE
     WHEN trim(bronze.assigned_to) = '' THEN NULL
     WHEN lower(trim(bronze.assigned_to)) IN ('n/a','null') THEN NULL
     WHEN trim(bronze.assigned_to) = '???' THEN NULL
     WHEN trim(bronze.assigned_to) ~ '^[A-Za-z0-9 \(\)\-\.,\'\"]+$' THEN trim(bronze.assigned_to)
     ELSE NULL
   END) AS assigned_to,

  -- building: trim, map '???' and empty -> NULL
  (CASE
     WHEN trim(bronze.building) = '' THEN NULL
     WHEN trim(bronze.building) = '???' THEN NULL
     ELSE trim(bronze.building)
   END) AS building,

  -- category: trim, map empty/'NULL' -> NULL
  (CASE
     WHEN trim(bronze.category) = '' THEN NULL
     WHEN lower(trim(bronze.category)) = 'null' THEN NULL
     ELSE trim(bronze.category)
   END) AS category,

  -- status: trim, map common placeholders to NULL
  (CASE
     WHEN trim(bronze.status) = '' THEN NULL
     WHEN lower(trim(bronze.status)) IN ('null','???','unknown') THEN NULL
     ELSE trim(bronze.status)
   END) AS status,

  -- priority: trim, map empty/??? -> NULL
  (CASE
     WHEN trim(bronze.priority) = '' THEN NULL
     WHEN trim(bronze.priority) = '???' THEN NULL
     ELSE trim(bronze.priority)
   END) AS priority,

  -- description: preserve text, empty -> NULL
  (CASE WHEN trim(bronze.description) = '' THEN NULL ELSE trim(bronze.description) END) AS description,

  -- resolution_notes: preserve text, empty -> NULL
  (CASE WHEN trim(bronze.resolution_notes) = '' THEN NULL ELSE trim(bronze.resolution_notes) END) AS resolution_notes,

  -- created_at: try multiple known formats (regex-checked) then NULL
  (CASE
     WHEN trim(bronze.created_at) ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$' THEN to_timestamp(trim(bronze.created_at), 'YYYY-MM-DD"T"HH24:MI:SS')
     WHEN trim(bronze.created_at) ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$' THEN to_timestamp(trim(bronze.created_at), 'YYYY-MM-DD HH24:MI:SS')
     WHEN trim(bronze.created_at) ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2} (am|pm|AM|PM)$' THEN to_timestamp(trim(bronze.created_at), 'MM/DD/YYYY HH12:MI AM')
     WHEN trim(bronze.created_at) ~ '^\d{4}-\d{2}-\d{2}$' THEN to_timestamp(trim(bronze.created_at), 'YYYY-MM-DD')
     WHEN trim(bronze.created_at) ~ '^\d{2}-[A-Za-z]{3}-\d{4} \d{2}:\d{2}$' THEN to_timestamp(trim(bronze.created_at), 'DD-Mon-YYYY HH24:MI')
     WHEN trim(bronze.created_at) ~ '^\d{2}/\d{2}/\d{4}$' THEN to_timestamp(trim(bronze.created_at), 'MM/DD/YYYY')
     WHEN trim(bronze.created_at) ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$' THEN to_timestamp(trim(bronze.created_at), 'DD-MM-YYYY HH24:MI:SS')
     WHEN trim(bronze.created_at) ~ '^\d{10}$' THEN to_timestamp(trim(bronze.created_at)::double precision)
     ELSE NULL
   END)::timestamp without time zone AS created_at,

  -- resolved_at: same set of patterns as created_at; handle blanks and '???' -> NULL
  (CASE
     WHEN trim(bronze.resolved_at) = '' THEN NULL
     WHEN trim(bronze.resolved_at) = '???' THEN NULL
     WHEN trim(bronze.resolved_at) ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$' THEN to_timestamp(trim(bronze.resolved_at), 'YYYY-MM-DD"T"HH24:MI:SS')
     WHEN trim(bronze.resolved_at) ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$' THEN to_timestamp(trim(bronze.resolved_at), 'YYYY-MM-DD HH24:MI:SS')
     WHEN trim(bronze.resolved_at) ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2} (am|pm|AM|PM)$' THEN to_timestamp(trim(bronze.resolved_at), 'MM/DD/YYYY HH12:MI AM')
     WHEN trim(bronze.resolved_at) ~ '^\d{4}-\d{2}-\d{2}$' THEN to_timestamp(trim(bronze.resolved_at), 'YYYY-MM-DD')
     WHEN trim(bronze.resolved_at) ~ '^\d{2}-[A-Za-z]{3}-\d{4} \d{2}:\d{2}$' THEN to_timestamp(trim(bronze.resolved_at), 'DD-Mon-YYYY HH24:MI')
     WHEN trim(bronze.resolved_at) ~ '^\d{2}/\d{2}/\d{4}$' THEN to_timestamp(trim(bronze.resolved_at), 'MM/DD/YYYY')
     WHEN trim(bronze.resolved_at) ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$' THEN to_timestamp(trim(bronze.resolved_at), 'DD-MM-YYYY HH24:MI:SS')
     WHEN trim(bronze.resolved_at) ~ '^\d{10}$' THEN to_timestamp(trim(bronze.resolved_at)::double precision)
     ELSE NULL
   END)::timestamp without time zone AS resolved_at,

  -- sla_hours: integer only when purely integer text
  (CASE
     WHEN trim(bronze.sla_hours) = '' THEN NULL
     WHEN lower(trim(bronze.sla_hours)) IN ('n/a','null') THEN NULL
     WHEN trim(bronze.sla_hours) ~ '^[-]?\d+$' THEN trim(bronze.sla_hours)::integer
     ELSE NULL
   END) AS sla_hours,

  -- cost: accept optional leading $ and optional decimal part, else NULL
  (CASE
     WHEN trim(bronze.cost) = '' THEN NULL
     WHEN lower(trim(bronze.cost)) IN ('n/a','tbd','null','error') THEN NULL
     WHEN trim(bronze.cost) ~ '^\$?-?\d+(\.\d{1,2})?$' THEN replace(trim(bronze.cost),'$','')::numeric(12,2)
     ELSE NULL
   END) AS cost

FROM bronze.tickets_raw AS bronze;