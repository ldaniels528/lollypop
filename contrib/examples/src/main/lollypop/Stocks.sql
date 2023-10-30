namespace 'demo.stocks'
//////////////////////////////////////////////////////////////////////////////////////
//      Stocks Demo
// include('./contrib/examples/src/main/lollypop/Stocks.sql')
//////////////////////////////////////////////////////////////////////////////////////

// create a macro to generate random stock quotes
drop if exists `tickers`
create macro `tickers` := 'tickers %e:total' {
    stdout <=== 'Generating {{total}} random stock quotes...\n'
    declare table myQuotes(symbol: String(4), exchange: String(6), lastSale: Double, lastSaleTime: DateTime)
    [1 to total].foreach((n: Int) => {
        insert into @@myQuotes (lastSaleTime, lastSale, exchange, symbol)
        select
            lastSaleTime: DateTime(),
            lastSale: scaleTo(500 * Random.nextDouble(0.99), 4),
            exchange: ['AMEX', 'NASDAQ', 'NYSE', 'OTCBB'][Random.nextInt(4)],
            symbol: Random.nextString(['A' to 'Z'], 4)
    })
    return @@myQuotes
}

// create a table variables with a capacity of 15000 rows
stocks = tickers 10

// display a limit of 5 rows
stdout <=== 'Sampling 5 quotes:\n'
stocks.show(5)
