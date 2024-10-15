CREATE OR REPLACE VIEW `focal-eon-405500.top10_stocks.stock_normalized_scaled` AS

WITH stats AS (
    SELECT
        AVG(Open) AS mean_open,
        STDDEV(Open) AS std_open
    FROM `focal-eon-405500.top10_stocks.top10stocks`
),
 min_max AS (
    SELECT
        MIN(Open) AS min_open, MAX(Open) AS max_open,
        MIN(Close) AS min_close, MAX(Close) AS max_close,
        MIN(Volume) AS min_volume, MAX(Volume) AS max_volume
    FROM `focal-eon-405500.top10_stocks.top10stocks`
)

SELECT
    Date,
    Symbol,
    (Open - stats.mean_open) / stats.std_open AS open_zscore,
    (Close - stats.mean_open) / stats.std_open AS close_zscore,
    (High - stats.mean_open) / stats.std_open AS high_zscore,
    (Low - stats.mean_open) / stats.std_open AS low_zscore,
    (Volume - AVG(Volume) OVER()) / STDDEV(Volume) OVER() AS volume_zscore,
    (Open - min_max.min_open) / (min_max.max_open - min_max.min_open) AS open_scaled,
    (Close - min_max.min_close) / (min_max.max_close - min_max.min_close) AS close_scaled,
    (Volume - min_max.min_volume) / (min_max.max_volume - min_max.min_volume) AS volume_scaled,
    LOG(Volume + 1) AS log_volume,
    CASE WHEN Symbol = 'WMT' THEN 1 ELSE 0 END AS is_WMT,
    CASE WHEN Symbol = 'AMZN' THEN 1 ELSE 0 END AS is_AMZN,
    CASE WHEN Symbol = 'UPS' THEN 1 ELSE 0 END AS is_UPS,
    CASE WHEN Symbol = 'FDX' THEN 1 ELSE 0 END AS is_FDX,
    CASE WHEN Symbol = 'HD' THEN 1 ELSE 0 END AS is_HD,
    CASE WHEN Symbol = 'UNH' THEN 1 ELSE 0 END AS is_UNH,
    CASE WHEN Symbol = 'CNXC' THEN 1 ELSE 0 END AS is_CNXC,
    CASE WHEN Symbol = 'TGT' THEN 1 ELSE 0 END AS is_TGT,
    CASE WHEN Symbol = 'KR' THEN 1 ELSE 0 END AS is_KR,
    CASE WHEN Symbol = 'MAR' THEN 1 ELSE 0 END AS is_MAR
FROM `focal-eon-405500.top10_stocks.top10stocks`, stats, min_max;


