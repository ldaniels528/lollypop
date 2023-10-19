namespace `demos.macro`
//////////////////////////////////////////////////////////////////////////////////////
//      macro Demo
// include './contrib/examples/src/main/qwery/MacroDemo.sql'
//////////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////
// macro 'jvm' - Provides JVM memory information
// Usage: jvm
////////////////////////////////////////////////////////////////////
macro 'jvm' := {
    import 'java.lang.Runtime'
    val rt = Runtime.getRuntime()
    val factor = 1024 * 1024
    val maxMemory = scaleTo(rt.maxMemory() / factor, 2) + " MB"
    val totalMemory = scaleTo(rt.totalMemory() / factor, 2) + " MB"
    val freeMemory = scaleTo(rt.freeMemory() / factor, 2) + " MB"
    val freeMemory_pct = scaleTo(100 * (rt.freeMemory() / rt.totalMemory()), 2) + " %"
    select
        availableProcessors: rt.availableProcessors(),
            maxMemory: maxMemory,
            totalMemory: totalMemory,
            freeMemory: freeMemory,
            freeMemory_pct: freeMemory_pct
}

////////////////////////////////////////////////////////////////////
// macro ls - List files
// Usage: ls '-al'
////////////////////////////////////////////////////////////////////
macro 'ls %e:options' := os_exec('ls {{ options }}')

////////////////////////////////////////////////////////////////////
// macro tickers - Generates random stock quotes for testing
// Usage: val stocks = tickers 100000
////////////////////////////////////////////////////////////////////
macro 'tickers %e:amount' := {
    assert (amount isCodecOf Int, 'total must be an integer')
    var cnt = 0
    declare table myQuotes(id: RowNumber, symbol: String(4), exchange: String(9), lastSale: Float, lastSaleTime: DateTime)[100000]
    while cnt < amount {
        cnt += 1
        insert into @@myQuotes (lastSaleTime, lastSale, exchange, symbol)
        select lastSaleTime: DateTime(),
               lastSale: scaleTo(150 * Random.nextDouble(0.99), 4),
               exchange: ['AMEX', 'NASDAQ', 'NYSE', 'OTCBB', 'OTHER_OTC'][Random.nextInt(5)],
               symbol: Random.nextString(['A' to 'Z'], 4)
    }
    myQuotes
}
