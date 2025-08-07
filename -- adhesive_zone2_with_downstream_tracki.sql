-- zone2_adhesive_downstream_failures.sql
-- 
-- Description:
-- This query identifies modules processed in Zone 2 during the adhesive application flowstep (2300)
-- and tracks whether they later failed quality checks in Zone 3 or Zone 4.
-- It returns key identifiers, timestamps, and time spent between process zones
-- to support root cause analysis for downstream failures (e.g. excessive adhesive).

-- Step 1: Get records from Zone 2 adhesive flowstep
WITH zone2_adhesive AS (
    SELECT
        d.line_id,
        d.timestamp AS zone2_timestamp,
        d.flowstep,
        d.pallet_id,
        d.carrier_id,
        d.description AS defect_description
    FROM
        defects_table d
    WHERE
        d.line_id = 'Z2'
        AND d.flowstep BETWEEN 2000 AND 2900
        AND d.flowstep = 2300
),

-- Step 2: Pull timestamps from downstream zones (Zone 3 and 4)
downstream_zones AS (
    SELECT
        pallet_id,
        MIN(CASE WHEN line_id = 'Z3' THEN timestamp END) AS zone3_timestamp,
        MIN(CASE WHEN line_id = 'Z4' THEN timestamp END) AS zone4_timestamp
    FROM
        defects_table
    WHERE
        line_id IN ('Z3', 'Z4')
    GROUP BY
        pallet_id
)

-- Step 3: Join upstream and downstream data and calculate time between zones
SELECT
    z2.line_id AS zone2_line_id,
    z2.zone2_timestamp,
    z2.pallet_id,
    z2.carrier_id,
    z2.defect_description,
    dz.zone3_timestamp,
    dz.zone4_timestamp,
    EXTRACT(EPOCH FROM dz.zone3_timestamp - z2.zone2_timestamp)/60 AS time_to_zone3_minutes,
    EXTRACT(EPOCH FROM dz.zone4_timestamp - z2.zone2_timestamp)/60 AS time_to_zone4_minutes
FROM
    zone2_adhesive z2
LEFT JOIN
    downstream_zones dz ON z2.pallet_id = dz.pallet_id
ORDER BY
    z2.zone2_timestamp DESC;


