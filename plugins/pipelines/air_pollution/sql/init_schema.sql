CREATE TABLE IF NOT EXISTS air_pollution (
    id SERIAL PRIMARY KEY,
    city VARCHAR(30),
    aqi SMALLINT,
    aqi_interpretation VARCHAR(15),
    measured_at TIMESTAMPTZ,
    day_of_week VARCHAR(10),
    time_of_day TIME,
    no REAL,
    no2 REAL,
    o3 REAL,
    so2 REAL,
    nh3 REAL,
    pm2_5 REAL,
    pm10 REAL
);