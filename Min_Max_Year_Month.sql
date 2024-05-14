USE [Stocks]
GO 
IF OBJECT_ID('tempdb..#MyTempTableMin') IS NOT NULL
DROP TABLE #MyTempTableMin;
IF OBJECT_ID('tempdb..#MyTempTableMax') IS NOT NULL
DROP TABLE #MyTempTableMax;
IF OBJECT_ID('tempdb..#FinalTable') IS NOT NULL
DROP TABLE #FinalTable;

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Min_Max_Year_Month]') AND type in (N'U'))
DROP TABLE [dbo].[Min_Max_Year_Month]
GO
CREATE TABLE [dbo].[Min_Max_Year_Month](
	[Ticker] [varchar](50) NULL,
	[Year] [int] NULL,
	[Month] [int] NULL,
	[Date_Min] [datetime] NULL,
	[Min] [float] NULL,
	[Date_Max] [datetime] NULL,
	[Max] [float] NULL
) ON [PRIMARY]
GO

;with cte as(
select
[Date]
,[Ticker]
,[Close]
,min([Close]) OVER(PARTITION BY YEAR(Date),MONTH(Date),Ticker) AS 'Min'
from Stocks_Float
where YEAR(Date) > 1999
),
cte_stocks as(
	select Ticker, Date,[Close] from Stocks_Float
)select DISTINCT cte.[Ticker]
, YEAR(cte.[Date]) as Year
, Month(cte.[Date]) as Month
, [Min]
, cte1.Date
into #MyTempTableMin
from cte
LEFT JOIN cte_stocks cte1 on cte.Ticker = cte1.Ticker and cte.[Min] = cte1.[Close]  and MONTH(cte.Date) = MONTH(cte1.Date)
where YEAR(cte1.Date) > 1999


;with cte as(
select
[Date]
,[Ticker]
,[Close]
,max([Close]) OVER(PARTITION BY YEAR(Date),MONTH(Date),Ticker) AS 'Max'
from Stocks_Float
where YEAR(Date) > 1999
),
cte_stocks as(
	select Ticker, Date,[Close] from Stocks_Float
)select DISTINCT cte.[Ticker]
, YEAR(cte.[Date]) as Year
, Month(cte.[Date]) as Month
, [Max]
, cte1.Date
into #MyTempTableMax
from cte
LEFT JOIN cte_stocks cte1 on cte.Ticker = cte1.Ticker and cte.[Max] = cte1.[Close]  and MONTH(cte.Date) = MONTH(cte1.Date)
where YEAR(cte1.Date) > 1999

select DISTINCT MyMin.Ticker, MyMin.Year, MyMin.Month
, MyMin.[Date] as 'Date_Min', MyMin.Min 
, MyMax.[Date] as 'Date_Max', MyMax.Max
into #FinalTable
from #MyTempTableMin MyMin
join #MyTempTableMax MyMax 
on MyMin.Ticker = MyMax.Ticker and
MyMin.Year = MyMax.Year and
MyMin.Month = MyMax.Month

insert into [Min_Max_Year_Month] ([Ticker],[Year],[Month],[Date_Min],[Min],[Date_Max],[Max])
Select DISTINCT * 
from #FinalTable
order by Ticker, Year, Month

IF OBJECT_ID('tempdb..#MyTempTableMin') IS NOT NULL
DROP TABLE #MyTempTableMin;
IF OBJECT_ID('tempdb..#MyTempTableMax') IS NOT NULL
DROP TABLE #MyTempTableMax;
IF OBJECT_ID('tempdb..#FinalTable') IS NOT NULL
DROP TABLE #FinalTable;

;WITH cte AS (
  SELECT*, 
     row_number() OVER(PARTITION BY Date_Min, Min, Date_Max, Max ORDER BY Date_Min desc) AS [rn]
  FROM Min_Max_Year_Month
)
delete from cte
where rn > 1
