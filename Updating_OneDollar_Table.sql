USE [Stocks] GO
DECLARE @Symbol VARCHAR(20)
DECLARE myCursorAllTicker CURSOR LOCAL FAST_FORWARD FOR 
select distinct s.[Ticker] from Stocks_Float s 
inner join [Stocks Info] si on s.Ticker = si.symbol where s.Ticker = 'AAPL' --si.sector IS NOT NULL order by Ticker
OPEN myCursorAllTicker
WHILE 1 = 1
BEGIN
	FETCH NEXT FROM myCursorAllTicker INTO @Symbol;
	IF @@FETCH_STATUS <> 0 BREAK;
	DECLARE @Ticker VARCHAR(20), @Ticker_Next VARCHAR(20), @Model VARCHAR(20),@Model_Next VARCHAR(20), @Price decimal(20,4), @Price_Next decimal(20,4)
	DECLARE @PriceAcum decimal(20,4) = 1, @Date date, @Date_Next date, @AppleDate datetime, @AppleDate_Next datetime, @Close decimal(20,4)
	DECLARE @Close_Next decimal(20,4), @Diff0 decimal(20,4), @Diff0_Next decimal(20,4), @Diff decimal(20,4), @Diff_Next decimal(20,4)
	DECLARE @Total decimal(20,4), @Sum decimal(20,4), @Sum_Next decimal(20,4), @Acum decimal(20,4), @Acum_Next decimal(20,4), @Flag decimal(20,4) = 0
	DECLARE myCursor CURSOR LOCAL FAST_FORWARD FOR
	SELECT	s.[Date],s.[Ticker],[Close],
			[Diff] = [Close] / (LAG ([Close],1) OVER (PARTITION BY s.[Ticker] ORDER BY s.[Date] ASC)),
			gb.[Acum]
	from Stocks_Float s join ( select [Ticker], [Date], [Acum] from [Stocks].[dbo].[View_Latest_OneDollar] where [Ticker] = @Symbol) gb
	on s.Ticker = gb.[Ticker] and s.[Date] >= gb.[Date]
	OPEN myCursor
	WHILE 1 = 1
	BEGIN
		FETCH NEXT FROM myCursor INTO @Date_Next,@Ticker_Next,@Close_Next,@Diff_Next, @Acum_Next;
		set @Ticker = @Ticker_Next;		set @Date = @Date_Next;		set @Diff = @Diff_Next;		set @Diff0 = @Diff0_Next;		set @Close = @Close_Next;
		IF @Flag = 0
			BEGIN
			set @Acum = @Acum_Next;set @Flag = 1;
			END
		ELSE
			set @Acum = @Acum;
		IF @@FETCH_STATUS <> 0 BREAK;
		IF @Diff <> 0 set @Acum = @Acum * ISNULL(@Diff, 0);
		--insert into [dbo].[StockOneDollar] VALUES(@Ticker,@Close,@Date,@Diff,@PriceAcum);
		PRINT @Ticker + ' '+ CAST(@Close as VARCHAR) + ' '+ CAST (@Date as VARCHAR) +  ' '+ CAST(@Diff as VARCHAR)  + ' '+ CAST(@Acum as VARCHAR)
	END
	CLOSE myCursor
	DEALLOCATE myCursor
END
CLOSE myCursorAllTicker