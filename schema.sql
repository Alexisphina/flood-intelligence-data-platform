CREATE TABLE IF NOT EXISTS pipeline_runs (
    pipeline_run_id TEXT PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    stations_processed INTEGER NOT NULL DEFAULT 0,
    readings_loaded INTEGER NOT NULL DEFAULT 0,
    rejected_records INTEGER NOT NULL DEFAULT 0,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS stations (
    station_reference TEXT PRIMARY KEY,
    label TEXT,
    river_name TEXT,
    catchment_name TEXT,
    town TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    easting DOUBLE PRECISION,
    northing DOUBLE PRECISION,
    status TEXT,
    date_opened TEXT,
    rloi_id TEXT,
    wiski_id TEXT,
    notation TEXT,
    source_station_url TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS measures (
    measure_id TEXT PRIMARY KEY,
    station_reference TEXT NOT NULL REFERENCES stations(station_reference),
    parameter TEXT,
    parameter_name TEXT,
    qualifier TEXT,
    unit_name TEXT,
    period_seconds DOUBLE PRECISION,
    source_measure_url TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS readings (
    measure_id TEXT NOT NULL REFERENCES measures(measure_id),
    station_reference TEXT NOT NULL REFERENCES stations(station_reference),
    reading_timestamp TIMESTAMPTZ NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    pipeline_run_id TEXT NOT NULL,
    batch_run_id TEXT,
    ingestion_timestamp TIMESTAMPTZ NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (measure_id, reading_timestamp)
);

CREATE INDEX IF NOT EXISTS ix_readings_station_timestamp
    ON readings (station_reference, reading_timestamp DESC);

CREATE INDEX IF NOT EXISTS ix_readings_timestamp
    ON readings (reading_timestamp DESC);

CREATE INDEX IF NOT EXISTS ix_readings_pipeline_run
    ON readings (pipeline_run_id);
