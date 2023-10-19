namespace `demo.ingest`
//////////////////////////////////////////////////////////////////////////////////////
//      INGESTION EXAMPLE
// include './contrib/examples/src/main/qwery/IngestDemo.sql'
//////////////////////////////////////////////////////////////////////////////////////
    
// create an external table to represent the input data
drop if exists StocksFileDump
create external table StocksFileDump (
    symbol: String(8),
    exchange: String(8),
    lastSale: Double,
    transactionTime: Long
) containing { format: 'CSV', location: './contrib/examples/stocks.csv', headers: false }
    
// create a work table to analyze the data
// NOTE: the DateTime automatically converts Longs to Dates
drop if exists Stocks
create table Stocks (
    symbol: String(8),
    exchange: String(8),
    lastSale: Double,
    transactionTime: DateTime
)

// populate the work table
insert into Stocks (symbol, exchange, lastSale, transactionTime)
select * from StocksFileDump

// create an index for fast reading
create index Stocks#symbol

// retrieve a record
select * from Stocks where symbol is 'JURY'
