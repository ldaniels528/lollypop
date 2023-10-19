//drop if exists StockQuotes

create table if not exists StockQuotes (
    symbol: String(5),
    exchange: String(6),
    lastSale: Double,
    lastSaleTime: DateTime
)

create index if not exists StockQuotes#symbol

//////////////////////////////////////////////////////////////////////////////////////
// setup the reference data
//////////////////////////////////////////////////////////////////////////////////////

// if the Stocks table is empty, populate it with random data
val stockQuotes = ns('StockQuotes')
if (count(stockQuotes) is 0) {
    truncate StockQuotes
    [1 to 5000].foreach((n: Int) => {
        insert into StockQuotes (lastSaleTime, lastSale, exchange, symbol)
        select
            lastSaleTime: DateTime(),
            lastSale: scaleTo(500 * Random.nextDouble(0.99), 4),
            exchange: ['AMEX', 'NASDAQ', 'NYSE', 'OTCBB'][Random.nextInt(4)],
            symbol: Random.nextString(['A' to 'Z'], 4)
    })
    stockQuotes.show(5)
}