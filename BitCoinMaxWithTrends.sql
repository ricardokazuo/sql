WITH RankedRows AS (
    SELECT [Date]
        ,[Crypto] AS [Ticker]
        ,[Open]
        ,[High]
        ,[Low]
        ,[Close]
        ,[Volume]
        ,[Dividends]
        ,[Stock Splits]
        ,ROW_NUMBER() OVER(PARTITION BY YEAR([Date]), MONTH([Date]) ORDER BY [Close]) as RowNum
    FROM [Stocks].[dbo].[Crypto_Float]
    WHERE [Crypto] = 'BTC-USD' AND YEAR([Date]) >= 2020
),
LowestCloseDays AS (
    SELECT [Date]
        ,[Ticker]
        ,[Open]
        ,[High]
        ,[Low]
        ,[Close]
        ,[Volume]
        ,[Dividends]
        ,[Stock Splits]
    FROM RankedRows
    WHERE RowNum = 1
),
CryptoData AS (
    SELECT cf.[Date]
        ,cf.[Crypto] AS [Ticker]
        ,cf.[Open]
        ,cf.[High]
        ,cf.[Low]
        ,cf.[Close]
        ,cf.[Volume]
        ,cf.[Dividends]
        ,cf.[Stock Splits]
    FROM [Stocks].[dbo].[Crypto_Float] cf
    JOIN LowestCloseDays lcd
    ON cf.[Crypto] = lcd.[Ticker]
    WHERE cf.[Date] BETWEEN DATEFROMPARTS(YEAR(lcd.[Date]), MONTH(lcd.[Date]), 1) AND lcd.[Date]
),
ITStocks AS (
    SELECT sf.[Date]
        ,sf.[Ticker]
        ,sf.[Open]
        ,sf.[High]
        ,sf.[Low]
        ,sf.[Close]
        ,sf.[Volume]
        ,sf.[Dividends]
        ,sf.[Stock Splits]
    FROM [Stocks].[dbo].[Stocks Info] si
    JOIN [Stocks].[dbo].[Stocks_Float] sf
    ON si.[symbol] = sf.[Ticker]
    WHERE si.[sector] IN ('Information Technology','Financials','Health Care')
),
JoinedData AS (
    SELECT IT.[Date]
        ,IT.[Ticker]
        ,IT.[Open]
        ,IT.[High]
        ,IT.[Low]
        ,IT.[Close]
        ,IT.[Volume]
        ,IT.[Dividends]
        ,IT.[Stock Splits]
    FROM ITStocks IT
    JOIN CryptoData CD
    ON IT.[Date] = CD.[Date]
),
AvgData AS (
    SELECT JD.[Date]
        ,JD.[Ticker]
        ,JD.[Open]
        ,JD.[High]
        ,JD.[Low]
        ,JD.[Close]
        ,JD.[Volume]
        ,JD.[Dividends]
        ,JD.[Stock Splits]
        ,AVG(JD.[Close]) OVER () AS average_all
        ,AVG(JD.[Close]) OVER (PARTITION BY JD.[Ticker]) AS average_ticker
    FROM JoinedData JD
)
SELECT AD.[Date]
    ,AD.[Ticker]
    ,AD.[Open]
    ,AD.[High]
    ,AD.[Low]
    ,AD.[Close]
    ,AD.[Volume]
    ,AD.[Dividends]
    ,AD.[Stock Splits]
    ,AD.average_all
    ,AD.average_ticker
    ,(AD.average_all - AD.average_ticker + AD.[Close]) AS Normalized
FROM AvgData AD
ORDER BY AD.[Date];

WITH RankedRows AS (
    SELECT [Date]
        ,[Crypto] AS [Ticker]
        ,[Open]
        ,[High]
        ,[Low]
        ,[Close]
        ,[Volume]
        ,[Dividends]
        ,[Stock Splits]
        ,ROW_NUMBER() OVER(PARTITION BY YEAR([Date]), MONTH([Date]) ORDER BY [Close] DESC) as RowNum
    FROM [Stocks].[dbo].[Crypto_Float]
    WHERE [Crypto] = 'BTC-USD' AND YEAR([Date]) >= 2020
),
HighestCloseDays AS (
    SELECT [Date]
        ,[Ticker]
        ,[Open]
        ,[High]
        ,[Low]
        ,[Close]
        ,[Volume]
        ,[Dividends]
        ,[Stock Splits]
    FROM RankedRows
    WHERE RowNum = 1
),
CryptoData AS (
    SELECT cf.[Date]
        ,cf.[Crypto] AS [Ticker]
        ,cf.[Open]
        ,cf.[High]
        ,cf.[Low]
        ,cf.[Close]
        ,cf.[Volume]
        ,cf.[Dividends]
        ,cf.[Stock Splits]
    FROM [Stocks].[dbo].[Crypto_Float] cf
    JOIN HighestCloseDays hcd
    ON cf.[Crypto] = hcd.[Ticker]
    WHERE cf.[Date] BETWEEN DATEFROMPARTS(YEAR(hcd.[Date]), MONTH(hcd.[Date]), 1) AND hcd.[Date]
),
ITStocks AS (
    SELECT sf.[Date]
        ,sf.[Ticker]
        ,sf.[Open]
        ,sf.[High]
        ,sf.[Low]
        ,sf.[Close]
        ,sf.[Volume]
        ,sf.[Dividends]
        ,sf.[Stock Splits]
    FROM [Stocks].[dbo].[Stocks Info] si
    JOIN [Stocks].[dbo].[Stocks_Float] sf
    ON si.[symbol] = sf.[Ticker]
    WHERE si.[sector] IN ('Information Technology','Financials','Health Care')
),
JoinedData AS (
    SELECT IT.[Date]
        ,IT.[Ticker]
        ,IT.[Open]
        ,IT.[High]
        ,IT.[Low]
        ,IT.[Close]
        ,IT.[Volume]
        ,IT.[Dividends]
        ,IT.[Stock Splits]
    FROM ITStocks IT
    JOIN CryptoData CD
    ON IT.[Date] = CD.[Date]
),
AvgData AS (
    SELECT JD.[Date]
        ,JD.[Ticker]
        ,JD.[Open]
        ,JD.[High]
        ,JD.[Low]
        ,JD.[Close]
        ,JD.[Volume]
        ,JD.[Dividends]
        ,JD.[Stock Splits]
        ,AVG(JD.[Close]) OVER () AS average_all
        ,AVG(JD.[Close]) OVER (PARTITION BY JD.[Ticker]) AS average_ticker
    FROM JoinedData JD
)
SELECT AD.[Date]
    ,AD.[Ticker]
    ,AD.[Open]
    ,AD.[High]
    ,AD.[Low]
    ,AD.[Close]
    ,AD.[Volume]
    ,AD.[Dividends]
    ,AD.[Stock Splits]
    ,AD.average_all
    ,AD.average_ticker
    ,(AD.average_all - AD.average_ticker + AD.[Close]) AS Normalized
FROM AvgData AD
