DROP TABLE [Stocks].[dbo].[Fact]
GO
CREATE TABLE [Stocks].[dbo].[Fact](
	[Date]			[datetime] NOT NULL,
	[Country]		[nvarchar](50) NULL,
	[Forecast]		[float] NULL,
	[Actuals]		[float] NULL,
	[Multiplier]	[float] NULL,
	[Rand]			[float] NULL,
	[Weekday]		[int] NULL
) ON [PRIMARY]
GO

declare @Actuals float
declare @Forecast float
declare @MyDate date
declare @Country nvarchar(50)
declare @Multiplier float
declare @Rand float
declare @Weekday int

set @MyDate = '2000-01-01'

WHILE ( @MyDate <= GETDATE())
BEGIN
	SET @Multiplier = (ABS(CHECKSUM(NEWID()) % (0.50 - (-0.50 ) + 0.5555)) + (-0.50 ))
	SET @Actuals = 1000.0 + floor(10000 * RAND(convert(varbinary, newid())))
	SET @Forecast = @Actuals + (@Actuals * @Multiplier)
	
	SET @Rand = Rand()
	SET @Weekday = DATEPART(DW,@MyDate)

	IF floor(6 * RAND(convert(varbinary, newid()))) = 0
	BEGIN SET @Country = 'Australia' END
	IF floor(6 * RAND(convert(varbinary, newid()))) = 1
	BEGIN SET @Country = 'Brazil' END
	IF floor(6 * RAND(convert(varbinary, newid()))) = 2
	BEGIN SET @Country = 'Canada' END
	IF floor(6 * RAND(convert(varbinary, newid()))) = 3
	BEGIN SET @Country = 'Denmark' END
	IF floor(6 * RAND(convert(varbinary, newid()))) = 4
	BEGIN SET @Country = 'Estonia' END
	IF floor(6 * RAND(convert(varbinary, newid()))) = 5
	BEGIN SET @Country = 'France' END
	
	IF @Country IS NOT NULL and @Weekday > 1 and @Weekday < 7
		insert into [Stocks].[dbo].[Fact] values (@MyDate, @Country, @Forecast, @Actuals, @Multiplier, @Rand, @Weekday) 
	SET @MyDate = DATEADD(DAY, 1, @MyDate)
END
GO

select * from [Stocks].[dbo].[Fact]
