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
Ticker,
YEAR([Date]) as Year,
MONTH([Date]) as Month,
min([Close]) as Min
from Stocks_Float
group by Ticker, YEAR([Date]), MONTH([Date])
),
cte_stocks as(
	select Ticker, Date,[Close] from Stocks_Float
)select
cte.Year
,cte.Month
,cte.[Ticker]
,cte.[Min]
,cte1.Date
,row_number() OVER(PARTITION BY Year, Month, cte.Ticker, Min ORDER BY Date asc) AS [rn]
into #MyTempTableMin
from cte
INNER JOIN cte_stocks cte1 on cte.Ticker = cte1.Ticker and cte.[Min] = cte1.[Close] and cte.Year = YEAR(cte1.Date) and cte.Month = MONTH(cte1.Date)
delete from #MyTempTableMin where rn > 1

;with cte as(
select 
Ticker,
YEAR([Date]) as Year,
MONTH([Date]) as Month,
max([Close]) as Max
from Stocks_Float
group by Ticker, YEAR([Date]), MONTH([Date])
),
cte_stocks as(
	select Ticker, Date,[Close] from Stocks_Float
)select
cte.Year
,cte.Month
,cte.[Ticker]
,cte.[Max]
,cte1.Date
,row_number() OVER(PARTITION BY Year, Month, cte.Ticker, Max ORDER BY Date desc) AS [rn]
into #MyTempTableMax
from cte
INNER JOIN cte_stocks cte1 on cte.Ticker = cte1.Ticker and cte.[Max] = cte1.[Close] and cte.Year = YEAR(cte1.Date) and cte.Month = MONTH(cte1.Date)
delete from #MyTempTableMax where rn > 1

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
