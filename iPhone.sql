USE [Stocks]
GO
IF OBJECT_ID('tempdb..#MyTempTable') IS NOT NULL
DROP TABLE #MyTempTable;
IF OBJECT_ID('tempdb..#MyiPhoneTable') IS NOT NULL
DROP TABLE #MyiPhoneTable;
IF OBJECT_ID('tempdb..#iPhone') IS NOT NULL
DROP TABLE #iPhone;
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[iPhone]') AND type in (N'U'))
DROP TABLE [dbo].[iPhone]
GO
CREATE TABLE [dbo].[iPhone](
	[Ticker] [varchar](50) NULL,
	[Model] [varchar](50) NULL,
	[Price] [float] NULL,
	[Date] [date] NULL,
	[Apple Date] [datetime] NULL,
	[Close] [float] NULL,
	[Diff] [float] NULL,
	[Sum] [float] NULL,
	[Acum] [float] NULL
) ON [PRIMARY]

;with cte_iphone as(
SELECT [Model]
      ,[Date]
      ,[Price]
  FROM [Stocks].[dbo].[Apple]
),
cte_aapl as(
SELECT * from Stocks_Float
where ticker = 'AAPL' and [Date] >= '2007-06-29'
)
select	cte_aapl.[Ticker],
		cte_iphone.[Model],
		cte_iphone.[Price],
		cte_iphone.[Date],
		cte_aapl.[Date] as 'Apple Date',
		cte_aapl.[Close]
into #MyiPhoneTable
from cte_iphone
RIGHT JOIN cte_aapl
ON cte_iphone.Date = cte_aapl.Date

select *
,case 
	when ([Close] / (LAG ([Close],1) OVER (PARTITION BY [Ticker] ORDER BY [Apple Date] ASC))) > 0.6 
	then [Close] / (LAG ([Close],1) OVER (PARTITION BY [Ticker] ORDER BY [Apple Date] ASC))
	else 1 - ([Close] / (LAG ([Close],1) OVER (PARTITION BY [Ticker] ORDER BY [Apple Date] ASC)))
end as Diff
,[Sum] = sum([Price]) OVER (PARTITION BY [Ticker] ORDER BY [Apple Date] ASC ROWS UNBOUNDED PRECEDING)
into #iPhone
from #MyiPhoneTable order by 'Apple Date'

DECLARE @Ticker VARCHAR(20)
DECLARE @Ticker_Next VARCHAR(20)
DECLARE @Model VARCHAR(20)
DECLARE @Model_Next VARCHAR(20)
DECLARE @Price real
DECLARE @Price_Next real
DECLARE @PriceAcum real 
DECLARE @Date date
DECLARE @Date_Next date
DECLARE @AppleDate datetime
DECLARE @AppleDate_Next datetime
DECLARE @Close real
DECLARE @Close_Next real
DECLARE @Diff real
DECLARE @Diff_Next real
DECLARE @Diff2 real
DECLARE @Total real
DECLARE @Total2 real
DECLARE @Sum real
DECLARE @Sum_Next real
DECLARE @Add real

set @Total = 0
set	@PriceAcum = 0

DECLARE myCursor CURSOR LOCAL FAST_FORWARD FOR
    SELECT * FROM #iPhone order by 'Apple Date' asc
OPEN myCursor
set @Ticker = NULL;
set @Model = NULL;
set @Price = NULL;
set @Date = NULL;
set @AppleDate = NULL;
set @Close = NULL;
set @Diff = NULL;
set @Sum = NULL;

WHILE 1 = 1
BEGIN
	FETCH NEXT FROM myCursor INTO @Ticker_Next,@Model_Next,@Price_Next,@Date_Next,@AppleDate_Next,@Close_Next,@Diff_Next,@Sum_Next;
	set @Ticker = @Ticker_Next;
	set @Model = @Model_Next;
	set @Price = @Price_Next;
	set @Date = @Date_Next;
	set @AppleDate = @AppleDate_Next;
	set @Close = @Close_Next;
	set @Diff = @Diff_Next;
	set @Sum = @Sum_Next;
	IF @@FETCH_STATUS <> 0 BREAK;
	set @Price = ISNULL(@Price, 0)
	set @Diff2 = @Diff;
	IF @PriceAcum > 0 
	BEGIN
		set @PriceAcum = @PriceAcum * @Diff
	END
	set	@PriceAcum = @PriceAcum + @Price;
	insert into [dbo].[iPhone] VALUES(@Ticker,@Model,@Price,@Date,@AppleDate,@Close,@Diff,@Sum,@PriceAcum);
	PRINT @Ticker + ' '+ CAST(@Price as VARCHAR) + ' '+ CAST (@AppleDate as VARCHAR) +  ' '+ CAST(@Diff as VARCHAR)  + ' '+ CAST(@Sum as VARCHAR)  + ' '+ CAST(@PriceAcum as VARCHAR)
END
PRINT @Total
CLOSE myCursor
DEALLOCATE myCursor
IF OBJECT_ID('tempdb..#MyTempTable') IS NOT NULL
DROP TABLE #MyTempTable;
IF OBJECT_ID('tempdb..#MyiPhoneTable') IS NOT NULL
DROP TABLE #MyiPhoneTable;
IF OBJECT_ID('tempdb..#iPhone') IS NOT NULL
DROP TABLE #iPhone;
GO
