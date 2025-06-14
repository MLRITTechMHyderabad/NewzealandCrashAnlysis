-- Step 1: Define Objective
-- Goal: Predict crashseverity (e.g., fatal, serious, minor, no injury) using available features.

-- Assumption: If your original dataset doesn't have a direct crashseverity column, we can create it using logic based on fatalcount, seriousinjurycount, and minorinjurycount.

-- step 2 creatinig training TABLE
CREATE OR REPLACE TABLE `newzealandcrashproject.crashnalysis.training_data` AS
SELECT
  *,
  CASE 
    WHEN fatalcount > 0 THEN 'Fatal'
    WHEN seriousinjurycount > 0 THEN 'Serious'
    WHEN minorinjurycount > 0 THEN 'Minor'
    ELSE 'No Injury'
  END AS crashseverity
FROM `newzealandcrashproject.crashnalysis.fact_crash`;
--  Step 3: Create ML Model in BigQuery
-- Using logistic regression or auto-ML classifier depending on your goal.


CREATE OR REPLACE MODEL `newzealandcrashproject.crashnalysis.crash_severity_model`
OPTIONS(
  model_type = 'logistic_reg',
  input_label_cols = ['crashseverity'],
  auto_class_weights = TRUE
) AS
SELECT
  date_id,
  location_id,
  weather_id,
  vehicle_id,
  road_id,
  fatalcount,
  seriousinjurycount,
  minorinjurycount,
  anyinjury,
  crashseverity
FROM `newzealandcrashproject.crashnalysis.training_data`;


-- Step 4: Evaluate the Model

SELECT
  *
FROM
  ML.EVALUATE(MODEL `newzealandcrashproject.crashnalysis.crash_severity_model`,
    (
      SELECT
        date_id,
        location_id,
        weather_id,
        vehicle_id,
        road_id,
        fatalcount,
        seriousinjurycount,
        minorinjurycount,
        anyinjury,
        crashseverity
      FROM `newzealandcrashproject.crashnalysis.training_data`
    )
  );
--   Step 5: Predict with the Model
sql
Copy
Edit
SELECT
  predicted_crashseverity,
  probability,
  *
FROM
  ML.PREDICT(MODEL `newzealandcrashproject.crashnalysis.crash_severity_model`,
    (
      SELECT
        date_id,
        location_id,
        weather_id,
        vehicle_id,
        road_id,
        fatalcount,
        seriousinjurycount,
        minorinjurycount,
        anyinjury
      FROM `newzealandcrashproject.crashnalysis.fact_crash`
      LIMIT 10
    )
  );