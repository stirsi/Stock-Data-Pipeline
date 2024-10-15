/*
-- Step 1: Create a seeded random value using Symbol and Date for consistency
WITH stock_data_with_seed AS (
    SELECT 
        *,
        -- Use FARM_FINGERPRINT to create a consistent "random" number based on Symbol and Date
        ABS(MOD(FARM_FINGERPRINT(CONCAT(Symbol, CAST(Date AS STRING))), 100)) AS random_seed
    FROM 
        `focal-eon-405500.top10_stocks.top10stocks`
)
-- Select the data (this is only for preview)
SELECT * FROM stock_data_with_seed;
*/

-- Step 2: Split into training (80%) and testing (20%) while stratifying by Symbol
-- Create the training table
CREATE OR REPLACE TABLE `focal-eon-405500.top10_stocks.stock_data_training` AS
SELECT 
    *
FROM 
    `focal-eon-405500.top10_stocks.top10stocks`
WHERE 
    ABS(MOD(FARM_FINGERPRINT(CONCAT(Symbol, CAST(Date AS STRING))), 100)) < 80;  -- 80% of data for training

-- Create the testing table
CREATE OR REPLACE TABLE `focal-eon-405500.top10_stocks.stock_data_testing` AS
SELECT 
    *
FROM 
    `focal-eon-405500.top10_stocks.top10stocks`
WHERE 
    ABS(MOD(FARM_FINGERPRINT(CONCAT(Symbol, CAST(Date AS STRING))), 100)) >= 80;  -- 20% of data for testing


--SELECT  FROM `focal-eon-405500.top10_stocks.top10stocks` LIMIT 1000