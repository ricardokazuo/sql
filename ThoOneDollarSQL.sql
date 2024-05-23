USE [Stocks]
GO

DECLARE @Symbol VARCHAR(20)
DECLARE myCursorAllTicker CURSOR LOCAL FAST_FORWARD FOR 
select distinct s.[Ticker] from Stocks_Float s 
inner join [Stocks Info] si on s.Ticker = si.symbol where si.sector = 'Financials' order by Ticker
OPEN myCursorAllTicker
WHILE 1 = 1
BEGIN
	FETCH NEXT FROM myCursorAllTicker INTO @Symbol;
	IF @@FETCH_STATUS <> 0 BREAK;
	DECLARE @Ticker VARCHAR(20), @Ticker_Next VARCHAR(20), @Model VARCHAR(20),@Model_Next VARCHAR(20), @Price decimal(20,4), @Price_Next decimal(20,4)
	DECLARE @PriceAcum decimal(20,4) = 1, @Date date, @Date_Next date, @AppleDate datetime, @AppleDate_Next datetime, @Close decimal(20,4)
	DECLARE @Close_Next decimal(20,4), @Diff0 decimal(20,4), @Diff0_Next decimal(20,4), @Diff decimal(20,4), @Diff_Next decimal(20,4)
	DECLARE @Total decimal(20,4), @Sum decimal(20,4), @Sum_Next decimal(20,4)
	DECLARE myCursor CURSOR LOCAL FAST_FORWARD FOR
		SELECT	[Date], 
				[Ticker], 
				[Close],
				[Diff] = [Close] / (LAG ([Close],1) OVER (PARTITION BY [Ticker] ORDER BY [Date] ASC))
		from Stocks_Float where Ticker = @Symbol and Date > '2000-01-01' order by Date asc
	OPEN myCursor
	WHILE 1 = 1
	BEGIN
		FETCH NEXT FROM myCursor INTO @Date_Next,@Ticker_Next,@Close_Next,@Diff_Next;
		set @Ticker = @Ticker_Next;
		set @Date = @Date_Next;
		set @Diff = @Diff_Next;
		set @Diff0 = @Diff0_Next;
		set @Close = @Close_Next;
		IF @@FETCH_STATUS <> 0 BREAK;
		IF @Diff <> 0 set @PriceAcum = @PriceAcum * ISNULL(@Diff, 0);
		insert into [dbo].[StockOneDollar] VALUES(@Ticker,@Close,@Date,@Diff,@PriceAcum);
		PRINT @Ticker + ' '+ CAST(@Close as VARCHAR) + ' '+ CAST (@Date as VARCHAR) +  ' '+ CAST(@Diff as VARCHAR)  + ' '+ CAST(@PriceAcum as VARCHAR)
	END
	CLOSE myCursor
	DEALLOCATE myCursor
END
CLOSE myCursorAllTicker
