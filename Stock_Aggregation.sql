CREATE OR REPLACE VIEW `project.dataset.stock_aggregates` AS
 SELECT
    Symbol,
    -- Aggregated values for Open
    ROUND(AVG(Open), 2) AS Avg_Open,
    MIN(Open) AS Min_Open,
    MAX(Open) AS Max_Open,
    ROUND(STDDEV(Open), 2) AS Std_Open,
    
    -- Aggregated values for Close
    ROUND(AVG(Close), 2) AS Avg_Close,
    MIN(Close) AS Min_Close,
    MAX(Close) AS Max_Close,
    ROUND(STDDEV(Close), 2) AS Std_Close,
    
    -- Aggregated values for Low
    ROUND(AVG(Low), 2) AS Avg_Low,
    MIN(Low) AS Min_Low,
    MAX(Low) AS Max_Low,
    ROUND(STDDEV(Low), 2) AS Std_Low,
    
    -- Aggregated values for High
    ROUND(AVG(High), 2) AS Avg_High,
    MIN(High) AS Min_High,
    MAX(High) AS Max_High,
    ROUND(STDDEV(High), 2) AS Std_High,
    
    -- Aggregated values for Volume
    ROUND(AVG(Volume)) AS Avg_Volume,
    SUM(Volume) AS Total_Volume,
    MIN(Volume) AS Min_Volume,
    MAX(Volume) AS Max_Volume,
    ROUND(STDDEV(Volume), 2) AS Std_Volume,
    
    -- Average Daily Return
    ROUND(AVG(daily_return), 4) AS Avg_Daily_Return,
    ROUND(MIN(daily_return), 4) AS Min_Daily_Return,
    ROUND(MAX(daily_return), 4) AS Max_Daily_Return,
    ROUND(STDDEV(daily_return), 2) AS Std_Daily_Return,
    
    -- Date range
    MIN(Date) AS Start_Date,
    MAX(Date) AS End_Date
    
  FROM `project.dataset.top10stocks`
  GROUP BY Symbol
