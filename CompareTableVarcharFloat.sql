USE [Stocks]
GO
;with cte as( -- < ----------------------------------------------------------First CTE
select *
,min([Close]) OVER(PARTITION BY YEAR(Date),Ticker) AS 'Min'
,max([Close]) OVER(PARTITION BY YEAR(Date),Ticker) AS 'Max'
,RowNum  = row_number() OVER(PARTITION BY Ticker, [Close], 'Temp' ORDER BY YEAR(Date) desc)
from Stocks
)
select	[Ticker],
		[Date],
		CASE (min([Close]) OVER(PARTITION BY YEAR(Date),Ticker)) WHEN [Close] 
			THEN (min([Close]) OVER(PARTITION BY YEAR(Date),Ticker)) END as Min,
		CASE (max([Close]) OVER(PARTITION BY YEAR(Date),Ticker)) WHEN [Close] 
			THEN (max([Close]) OVER(PARTITION BY YEAR(Date),Ticker)) END as Max
INTO #MyTempTable -- < ----------------------------------------------------------inset into a temp table
from cte
where  ([Close] = Min  or [Close] = Max) and [RowNum] < 2
order by Ticker, Date
select * 
INTO #MyUnpivot -- < ----------------------------------------------------------inset into a temp table
from #MyTempTable
UNPIVOT(
[Close] FOR Type IN (Min, MAX)
) Temp
ORDER BY Ticker, Date

SELECT MMax.[Ticker]
      ,MMax.[Date] as 'Date Max'
      ,MMax.[Close] as 'Max'
	  ,YEAR(MMax.[Date]) as 'Year'
      ,MMin.[Close] as 'Min'
      ,MMin.[Date] as 'Date Min'
INTO #Final_Stocks -- < ----------------------------------------------------------inset into a temp table
  FROM #MyUnpivot as MMax
  JOIN
  (SELECT [Ticker]
      ,[Date]
      ,[Close]
	  ,YEAR([Date]) as 'Year'
  FROM #MyUnpivot) as MMin
  on MMax.[Ticker] = MMin.[Ticker] and YEAR(MMax.[Date]) = YEAR(MMin.[Date])
  WHERE CONVERT(FLOAT, REPLACE(MMax.[Close], CHAR(0), '')) > 0
  and CONVERT(FLOAT, REPLACE(MMin.[Close], CHAR(0), '')) > 0
  and CONVERT(FLOAT, REPLACE(MMax.[Close], CHAR(0), '')) - 
  CONVERT(FLOAT, REPLACE(MMin.[Close], CHAR(0), '')) > 0
  ORDER BY Ticker, Year
  
IF OBJECT_ID('tempdb..#MyTempTable') IS NOT NULL
DROP TABLE #MyTempTable;
IF OBJECT_ID('tempdb..#MyUnpivot') IS NOT NULL
DROP TABLE #MyUnpivot;

;with cte as( -- < ----------------------------------------------------------Second CTE
select *
,min([Close]) OVER(PARTITION BY YEAR(Date),Ticker) AS 'Min'
,max([Close]) OVER(PARTITION BY YEAR(Date),Ticker) AS 'Max'
,RowNum  = row_number() OVER(PARTITION BY Ticker, [Close], 'Temp' ORDER BY YEAR(Date) desc)
from Stocks_Float
)
select	[Ticker],
		[Date],
		CASE (min([Close]) OVER(PARTITION BY YEAR(Date),Ticker)) WHEN [Close] 
			THEN (min([Close]) OVER(PARTITION BY YEAR(Date),Ticker)) END as Min,
		CASE (max([Close]) OVER(PARTITION BY YEAR(Date),Ticker)) WHEN [Close] 
			THEN (max([Close]) OVER(PARTITION BY YEAR(Date),Ticker)) END as Max
INTO #MyTempTable_Float -- < ----------------------------------------------------------inset into a temp table
from cte
where  ([Close] = Min  or [Close] = Max) and [RowNum] < 2
order by Ticker, Date
select * 
INTO #MyUnpivot_Float -- < ----------------------------------------------------------inset into a temp table
from #MyTempTable_Float
UNPIVOT(
[Close] FOR Type IN (Min, MAX)
) Temp
ORDER BY Ticker, Date

SELECT MMax.[Ticker]
      ,MMax.[Date] as 'Date Max'
      ,MMax.[Close] as 'Max'
	  ,YEAR(MMax.[Date]) as 'Year'
      ,MMin.[Close] as 'Min'
      ,MMin.[Date] as 'Date Min'
INTO #Final_Stocks_Float -- < ----------------------------------------------------------inset into a temp table
  FROM #MyUnpivot_Float as MMax
  JOIN
  (SELECT [Ticker]
      ,[Date]
      ,[Close]
	  ,YEAR([Date]) as 'Year'
  FROM #MyUnpivot_Float) as MMin
  on MMax.[Ticker] = MMin.[Ticker] and YEAR(MMax.[Date]) = YEAR(MMin.[Date])
  WHERE MMax.[Close] > 0  and MMin.[Close] > 0  and MMax.[Close] -  MMin.[Close] > 0
  ORDER BY Ticker, Year

select MyChar.[Date Min] as 'Date_VARCHAR', MyFloat.[Date Min] as 'Date_FLOAT', MyChar.Ticker, 
		MyChar.Min as 'MIN_VARCHAR', MyFloat.Min as 'MIN_FLOAT', MyChar.Max as 'MAX_VARCHAR', MyFloat.Max as 'MAX_FLOAT',
		CAST(MyChar.Min as DECIMAL(24, 3)) - CAST(MyFloat.Min as DECIMAL(24, 3)) as 'Min Diff',
		CASE CAST(MyChar.Min as DECIMAL(24, 3)) WHEN CAST(MyFloat.Min as DECIMAL(24, 3)) THEN 'OK' ELSE 'Not OK' END as 'Min Equal',
		CAST(MyChar.Max as DECIMAL(24, 3)) - CAST(MyFloat.Max as DECIMAL(24, 3)) as 'Max Diff',
		CASE CAST(MyChar.Max as DECIMAL(24, 3)) WHEN CAST(MyFloat.Max as DECIMAL(24, 3)) THEN 'OK' ELSE 'Not OK' END as 'Max Equal'
from #Final_Stocks MyChar
JOIN 
(select * from #Final_Stocks_Float ) as MyFloat
on ( MyChar.Ticker = MyFloat.Ticker and MyChar.Year = MyFloat.Year )
order by 'Min Equal', Ticker

IF OBJECT_ID('tempdb..#MyTempTable_Float') IS NOT NULL
DROP TABLE #MyTempTable_Float;
IF OBJECT_ID('tempdb..#MyUnpivot_Float') IS NOT NULL
DROP TABLE #MyUnpivot_Float;
IF OBJECT_ID('tempdb..#Final_Stocks') IS NOT NULL
DROP TABLE #Final_Stocks;
IF OBJECT_ID('tempdb..#Final_Stocks_Float') IS NOT NULL
DROP TABLE #Final_Stocks_Float;