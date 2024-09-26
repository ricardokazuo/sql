        WITH 
        RankedRows AS (
            SELECT 
                "Date", "Ticker", "Open", "High", "Low", "Close", "Volume",
                ROW_NUMBER() OVER(PARTITION BY DATE_TRUNC('MONTH', "Date") ORDER BY "Close" DESC) as RowNum
            FROM Crypto_Float
            WHERE "Ticker" = 'BTC-USD' AND "Date" >= DATE '2020-01-01'
        ),
        HighestCloseDays AS (
            SELECT *
            FROM RankedRows
            WHERE RowNum = 1
        ),
        ITStocks AS (
            SELECT 
                sf."Date", sf."Ticker", sf."Open", sf."High", sf."Low", sf."Close", sf."Volume",
                si."sector"
            FROM Stocks_Float sf
            JOIN Stocks_Info si ON si."symbol" = sf."Ticker"
            WHERE si."sector" IN ('Information Technology', 'Financials', 'Health Care')
                AND sf."Date" >= DATE '2020-01-01'
        ),
        JoinedData AS (
            SELECT 
                IT."Date", IT."Ticker", IT."Open", IT."High", IT."Low", IT."Close", IT."Volume",
                IT."sector"
            FROM ITStocks IT
            JOIN HighestCloseDays HCD ON DATE_TRUNC('MONTH', IT."Date") = DATE_TRUNC('MONTH', HCD."Date")
            WHERE IT."Date" <= HCD."Date"
        )
        SELECT 
            JD."Date", 
            JD."Ticker", 
            JD."Open", 
            JD."High", 
            JD."Low", 
            JD."Close", 
            JD."Volume",
            AVG(JD."Close") OVER () AS average_all,
            AVG(JD."Close") OVER (PARTITION BY JD."Ticker") AS average_ticker,
            (AVG(JD."Close") OVER () - AVG(JD."Close") OVER (PARTITION BY JD."Ticker") + JD."Close") AS Normalized
        FROM JoinedData JD
        ORDER BY JD."Date", JD."Ticker";
