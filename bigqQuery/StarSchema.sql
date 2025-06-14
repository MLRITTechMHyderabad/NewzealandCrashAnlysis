CREATE OR REPLACE TABLE `newzealandcrashproject.crashnalysis.dim_date` AS
SELECT
  ROW_NUMBER() OVER (ORDER BY crashyear, crashfinancialstartyear) AS date_id,
  crashyear,
  crashfinancialstartyear
FROM (
  SELECT DISTINCT crashyear, crashfinancialstartyear
  FROM `newzealandcrashproject.crashnalysis.crash_analysis_data`
  WHERE crashyear IS NOT NULL
);

CREATE OR REPLACE TABLE `newzealandcrashproject.crashnalysis.dim_location` AS
SELECT
  ROW_NUMBER() OVER () AS location_id,
  crashlocation1,
  crashlocation2,

FROM (
  SELECT DISTINCT crashlocation1, crashlocation2
  FROM `newzealandcrashproject.crashnalysis.crash_analysis_data`
);

CREATE OR REPLACE TABLE `newzealandcrashproject.crashnalysis.dim_weather` AS
SELECT
  ROW_NUMBER() OVER () AS weather_id,
  weathera,
  light
FROM (
  SELECT DISTINCT weathera, light
  FROM `newzealandcrashproject.crashnalysis.crash_analysis_data`
);

CREATE OR REPLACE TABLE `newzealandcrashproject.crashnalysis.dim_vehicle` AS
SELECT
  ROW_NUMBER() OVER () AS vehicle_id,
  bus, carStationWagon, motorcycle, othervehicletype
FROM (
  SELECT DISTINCT bus, carStationWagon, motorcycle, othervehicletype
  FROM `newzealandcrashproject.crashnalysis.crash_analysis_data`
);

CREATE OR REPLACE TABLE `newzealandcrashproject.crashnalysis.dim_road` AS
SELECT
  ROW_NUMBER() OVER () AS road_id,
  speedlimit, roadsurface, roadcharacter, roadworks, trafficcontrol
FROM (
  SELECT DISTINCT speedlimit, roadsurface, roadcharacter, roadworks, trafficcontrol
  FROM `newzealandcrashproject.crashnalysis.crash_analysis_data`
);
CREATE OR REPLACE TABLE `newzealandcrashproject.crashnalysis.fact_crash` AS
SELECT
  
  d.date_id,
  l.location_id,
  w.weather_id,
  v.vehicle_id,
  r.road_id,
  c.fatalcount,
  c.seriousinjurycount,
  c.minorinjurycount,
  c.anyinjury
FROM `newzealandcrashproject.crashnalysis.crash_analysis_data` c

-- Join with dim_date
JOIN `newzealandcrashproject.crashnalysis.dim_date` d
  ON c.crashyear = d.crashyear AND c.crashfinancialstartyear = d.crashfinancialstartyear

-- Join with dim_location
JOIN `newzealandcrashproject.crashnalysis.dim_location` l
  ON c.crashlocation1 = l.crashlocation1 AND c.crashlocation2 = l.crashlocation2

-- Join with dim_weather
JOIN `newzealandcrashproject.crashnalysis.dim_weather` w
  ON c.weathera = w.weathera AND c.light = w.light

-- Join with dim_vehicle
JOIN `newzealandcrashproject.crashnalysis.dim_vehicle` v
  ON c.bus = v.bus AND c.carStationWagon = v.carStationWagon AND c.motorcycle = v.motorcycle AND c.othervehicletype = v.othervehicletype

-- Join with dim_road
JOIN `newzealandcrashproject.crashnalysis.dim_road` r
  ON c.speedlimit = r.speedlimit
     AND c.roadsurface = r.roadsurface
     AND c.roadcharacter = r.roadcharacter
     AND c.roadworks = r.roadworks
     AND c.trafficcontrol = r.trafficcontrol;


SELECT 
  f.fatalcount,
  f.seriousinjurycount,
  f.minorinjurycount,
  d.crashyear,
  d.crashfinancialstartyear
FROM `newzealandcrashproject.crashnalysis.fact_crash` f
JOIN `newzealandcrashproject.crashnalysis.dim_date` d
  ON f.date_id = d.date_id
LIMIT 10;
