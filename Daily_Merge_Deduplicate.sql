MERGE `focal-eon-405500.top10_stocks.top10stocks` AS t
USING (
  SELECT DISTINCT Date, Symbol, Open, High, Low, Close, Volume
  FROM `focal-eon-405500.top10_stocks.top10stocks_daily`
) AS s
ON t.Date = s.Date AND t.Symbol = s.Symbol
WHEN MATCHED THEN
  UPDATE SET
    t.Open = s.Open,
    t.High = s.High,
    t.Low = s.Low,
    t.Close = s.Close,
    t.Volume = s.Volume
WHEN NOT MATCHED THEN
  INSERT (Date, Symbol, Open, High, Low, Close, Volume, day, month, year, daily_return, UniqueID, Day_Name)
  VALUES (
    s.Date, 
    s.Symbol, 
    s.Open, 
    s.High, 
    s.Low, 
    s.Close, 
    s.Volume, 
    EXTRACT(DAY FROM s.Date),  -- day
    EXTRACT(MONTH FROM s.Date), -- month
    EXTRACT(YEAR FROM s.Date),  -- year
    (s.Close - s.Open) / s.Open,  -- daily_return calculation
    GENERATE_UUID(),  -- Generate unique ID for each record
    FORMAT_DATE('%A', s.Date) -- Day_Name (e.g., Monday, Tuesday)
  );
