-- adhesive_zone2_with_downstream_tracking.sql

-- Step 1: Get Zone 2 adhesive application data
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

-- Step 2: Get downstream (Z3 and Z4) timestamps for the same pallet
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

-- Step 3: Join both sets to compute time spent and return final output
SELECT
    z2.line_id AS zone2_line_id,
    z2.zone2_timestamp,
    z2.pallet_id,
    z2.carrier_id,
    z2.defect_description,
    dz.zone3_timestamp,
    dz.zone4_timestamp,
    -- Calculate duration from zone 2 to 3 and 4
    EXTRACT(EPOCH FROM dz.zone3_timestamp - z2.zone2_timestamp)/60 AS time_to_zone3_minutes,
    EXTRACT(EPOCH FROM dz.zone4_timestamp - z2.zone2_timestamp)/60 AS time_to_zone4_minutes
FROM
    zone2_adhesive z2
LEFT JOIN
    downstream_zones dz ON z2.pallet_id = dz.pallet_id
ORDER BY
    z2.zone2_timestamp DESC;