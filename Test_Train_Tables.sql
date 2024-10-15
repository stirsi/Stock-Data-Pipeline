
CREATE OR REPLACE TABLE `project.dataset.stock_data_training` AS
SELECT 
    *
FROM 
    `project.dataset.top10stocks`
WHERE 
    ABS(MOD(FARM_FINGERPRINT(CONCAT(Symbol, CAST(Date AS STRING))), 100)) < 80;  -- 80% of data for training

-- Create the testing table
CREATE OR REPLACE TABLE `project.dataset.stock_data_testing` AS
SELECT 
    *
FROM 
    `project.dataset.top10stocks`
WHERE 
    ABS(MOD(FARM_FINGERPRINT(CONCAT(Symbol, CAST(Date AS STRING))), 100)) >= 80;  -- 20% of data for testing

