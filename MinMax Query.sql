DELETE from [Stocks].[dbo].[Crypto_Min_Max_Close]
GO

DECLARE @MinYear INT 
DECLARE @MaxYear INT 
SET @MinYear= (SELECT YEAR(MIN(Date)) AS MinYear FROM [Stocks].[dbo].[Crypto])
SET @MaxYear= (SELECT YEAR(MAX(Date)) AS MinYear FROM [Stocks].[dbo].[Crypto])
WHILE ( @MinYear <= @MaxYear)
BEGIN
	;WITH cte AS (
	SELECT*, row_number() OVER(PARTITION BY Crypto, 'Min Close' ORDER BY Date desc) AS [rn]
	FROM 
	(
		select b.[Crypto], [Date], [Close] as 'Min Close' FROM [Stocks].[dbo].[Crypto]  as a 
		join 
		(select Crypto, min([Close]) Min FROM [Stocks].[dbo].[Crypto] where YEAR(Date) = @MinYear  group by [Crypto]) as b 
		on a.Crypto = b.Crypto and a.[Close] = b.Min
	) as temp)
	insert into [Stocks].[dbo].[Crypto_Min_Max_Close] (Crypto,Date,[Close],[Type]) 
	select Crypto, Date, [Min Close], 'Min' from cte WHERE [rn] = 1
  SET @MinYear  = @MinYear  + 1
END

SET @MinYear= (SELECT YEAR(MIN(Date)) AS MinYear FROM [Stocks].[dbo].[Crypto])
WHILE ( @MinYear <= @MaxYear)
BEGIN
	;WITH cte AS (
	SELECT*, row_number() OVER(PARTITION BY Crypto, 'Max Close' ORDER BY Date desc) AS [rn]
	FROM 
	(
		select b.[Crypto], [Date], [Close] as 'Max Close' FROM [Stocks].[dbo].[Crypto]  as a 
		join 
		(select Crypto, max([Close]) Max FROM [Stocks].[dbo].[Crypto] where YEAR(Date) = @MinYear  group by [Crypto]) as b 
		on a.Crypto = b.Crypto and a.[Close] = b.Max
	) as temp)
	insert into [Stocks].[dbo].[Crypto_Min_Max_Close] (Crypto,Date,[Close],[Type]) 
	select Crypto, Date, [Max Close], 'Max' from cte WHERE [rn] = 1
  SET @MinYear  = @MinYear  + 1
END