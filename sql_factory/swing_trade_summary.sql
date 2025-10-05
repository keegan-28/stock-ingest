WITH trade_summary AS (
    SELECT
        ticker,
        SUM(CASE WHEN order_side = 'BUY' THEN qty ELSE 0 END) AS total_bought,
        SUM(CASE WHEN order_side = 'SELL' THEN qty ELSE 0 END) AS total_sold,
        SUM(CASE WHEN order_side = 'BUY' THEN qty * price ELSE 0 END)
            / NULLIF(SUM(CASE WHEN order_side = 'BUY' THEN qty ELSE 0 END), 0)
            AS avg_buy_price,
        SUM(CASE WHEN order_side = 'SELL' THEN qty * price ELSE 0 END)
            / NULLIF(SUM(CASE WHEN order_side = 'SELL' THEN qty ELSE 0 END), 0)
            AS avg_sell_price,
        SUM(fees) AS total_fees
    FROM tradeaction 
    GROUP BY ticker
),
latest_price AS (
    SELECT DISTINCT ON (ticker)
        ticker,
        close AS last_close,
        volume AS latest_volume,
        timestamp::date AS last_date
    FROM stockticks
    ORDER BY ticker, timestamp DESC
)
SELECT
    t.ticker,
    t.total_bought,
    t.total_sold,
    (t.total_bought - t.total_sold) AS net_position,
    t.avg_buy_price,
    t.avg_sell_price,
    p.last_close,
    p.latest_volume,
    t.total_fees,

    ROUND(
        (COALESCE(t.total_sold * (t.avg_sell_price - t.avg_buy_price), 0) - t.total_fees)::numeric,
        2
    ) AS realised_pnl,

    ROUND(
        ((t.total_bought - t.total_sold) * (p.last_close - t.avg_buy_price))::numeric,
        2
    ) AS unrealised_pnl,

    ROUND(
        (
            COALESCE(t.total_sold * (t.avg_sell_price - t.avg_buy_price), 0)
            + ((t.total_bought - t.total_sold) * (p.last_close - t.avg_buy_price))
            - t.total_fees
        )::numeric,
        2
    ) AS total_pnl,

    p.last_date AS market_date
FROM trade_summary t
LEFT JOIN latest_price p ON t.ticker = p.ticker
ORDER BY t.ticker;
