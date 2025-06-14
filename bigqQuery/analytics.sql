-- Crash Severity Distribution
SELECT crashseverity, COUNT(*) AS total_crashes
FROM `newzealandcrashproject.crashnalysis.training_data`
GROUP BY crashseverity
ORDER BY total_crashes DESC;

-- Top 5 Locations with Most Fatal Crashes
SELECT l.crashlocation1, COUNT(*) AS fatal_crashes
FROM `newzealandcrashproject.crashnalysis.fact_crash` f
JOIN `newzealandcrashproject.crashnalysis.dim_location` l USING(location_id)
WHERE f.fatalcount > 0
GROUP BY l.crashlocation1
ORDER BY fatal_crashes DESC
LIMIT 5;

-- Year pattern of serious injuries
SELECT d.crashyear, SUM(f.seriousinjurycount) AS total_serious_injuries
FROM `newzealandcrashproject.crashnalysis.fact_crash` f
JOIN `newzealandcrashproject.crashnalysis.dim_date` d USING(date_id)
GROUP BY d.crashyear
ORDER BY d.crashyear;
--injury ration
SELECT
  SUM(fatalcount) AS fatal,
  SUM(seriousinjurycount) AS serious,
  SUM(minorinjurycount) AS minor
FROM `newzealandcrashproject.crashnalysis.fact_crash`;

--weather and light effects on crash count
SELECT w.weathera, w.light, COUNT(*) AS crash_count
FROM `newzealandcrashproject.crashnalysis.fact_crash` f
JOIN `newzealandcrashproject.crashnalysis.dim_weather` w USING(weather_id)
GROUP BY w.weathera, w.light
ORDER BY crash_count DESC;

--crashesh occured at night
SELECT COUNT(*) AS night_crashes
FROM `newzealandcrashproject.crashnalysis.fact_crash` f
JOIN `newzealandcrashproject.crashnalysis.dim_weather` w USING(weather_id)
WHERE w.light = 'Dark';

--crashes occured by speed limit
SELECT r.speedlimit, COUNT(*) AS crashes
FROM `newzealandcrashproject.crashnalysis.fact_crash` f
JOIN `newzealandcrashproject.crashnalysis.dim_road` r USING(road_id)
GROUP BY r.speedlimit
ORDER BY crashes DESC;

--crashes by road surface
SELECT r.roadsurface, COUNT(*) AS crashes
FROM `newzealandcrashproject.crashnalysis.fact_crash` f
JOIN `newzealandcrashproject.crashnalysis.dim_road` r USING(road_id)
GROUP BY r.roadsurface
ORDER BY crashes DESC;

--Crashes Involving Buses
SELECT COUNT(*) AS bus_crashes
FROM `newzealandcrashproject.crashnalysis.fact_crash` f
JOIN `newzealandcrashproject.crashnalysis.dim_vehicle` v USING(vehicle_id)
WHERE v.bus > 0;

--vehicle types involved in crashes
SELECT
  SUM(v.carStationWagon) AS cars,
  SUM(v.motorcycle) AS motorcycles,
  SUM(v.bus) AS buses,
  SUM(v.othervehicletype) AS others
FROM `newzealandcrashproject.crashnalysis.fact_crash` f
JOIN `newzealandcrashproject.crashnalysis.dim_vehicle` v USING(vehicle_id);

--crashesh occured at location top 10
SELECT crashlocation1, COUNT(*) AS total_crashes
FROM `newzealandcrashproject.crashnalysis.fact_crash` f
JOIN `newzealandcrashproject.crashnalysis.dim_location` l USING(location_id)
GROUP BY crashlocation1
ORDER BY total_crashes DESC
LIMIT 10;

--dangerous locations
SELECT crashlocation1, COUNT(*) AS injury_crashes
FROM `newzealandcrashproject.crashnalysis.fact_crash` f
JOIN `newzealandcrashproject.crashnalysis.dim_location` l USING(location_id)
WHERE seriousinjurycount > 0
GROUP BY crashlocation1
ORDER BY injury_crashes DESC
LIMIT 10;

--Crashes Per Year
SELECT crashyear, COUNT(*) AS total_crashes
FROM `newzealandcrashproject.crashnalysis.dim_date`
JOIN `newzealandcrashproject.crashnalysis.fact_crash` USING(date_id)
GROUP BY crashyear
ORDER BY crashyear;

-- Financial Year Trends
SELECT crashfinancialstartyear, COUNT(*) AS crash_count
FROM `newzealandcrashproject.crashnalysis.dim_date`
JOIN `newzealandcrashproject.crashnalysis.fact_crash` USING(date_id)
GROUP BY crashfinancialstartyear
ORDER BY crashfinancialstartyear;

--Weather vs. Fatal Crashes
SELECT w.weathera, COUNT(*) AS fatal_crashes
FROM `newzealandcrashproject.crashnalysis.fact_crash` f
JOIN `newzealandcrashproject.crashnalysis.dim_weather` w USING(weather_id)
WHERE fatalcount > 0
GROUP BY w.weathera
ORDER BY fatal_crashes DESC;

--Road Type and Serious Injuries
SELECT r.roadcharacter, SUM(f.seriousinjurycount) AS serious_injuries
FROM `newzealandcrashproject.crashnalysis.fact_crash` f
JOIN `newzealandcrashproject.crashnalysis.dim_road` r USING(road_id)
GROUP BY r.roadcharacter
ORDER BY serious_injuries DESC;

--Injury Ratio per Crash
SELECT
  AVG(fatalcount + seriousinjurycount + minorinjurycount) AS avg_injuries_per_crash
FROM `newzealandcrashproject.crashnalysis.fact_crash`;

-- percenatge of Crashes Involving Roadworks
SELECT
  ROUND(SUM(CASE WHEN r.roadworks = 1 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS percent_with_roadworks
FROM `newzealandcrashproject.crashnalysis.fact_crash` f
JOIN `newzealandcrashproject.crashnalysis.dim_road` r USING(road_id);




