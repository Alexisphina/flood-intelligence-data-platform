-- Expected counts from the demonstrated processed run:
SELECT COUNT(*) AS station_count FROM stations;
SELECT COUNT(*) AS measure_count FROM measures;
SELECT COUNT(*) AS reading_count FROM readings;

-- Must return zero rows: duplicate business keys.
SELECT measure_id, reading_timestamp, COUNT(*)
FROM readings
GROUP BY measure_id, reading_timestamp
HAVING COUNT(*) > 1;

-- Stations with no recent readings; useful for stale-station monitoring.
SELECT
    s.station_reference,
    s.label,
    MAX(r.reading_timestamp) AS latest_reading,
    NOW() - MAX(r.reading_timestamp) AS data_age
FROM stations AS s
LEFT JOIN readings AS r
    ON r.station_reference = s.station_reference
GROUP BY s.station_reference, s.label
HAVING MAX(r.reading_timestamp) IS NULL
    OR MAX(r.reading_timestamp) < NOW() - INTERVAL '24 hours'
ORDER BY latest_reading NULLS FIRST;

-- Latest observations for analyst use.
SELECT
    r.station_reference,
    s.label,
    m.parameter_name,
    m.qualifier,
    m.unit_name,
    r.reading_timestamp,
    r.value
FROM readings AS r
JOIN stations AS s USING (station_reference)
JOIN measures AS m USING (measure_id)
ORDER BY r.reading_timestamp DESC
LIMIT 100;
