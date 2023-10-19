nodeAPI(port, '/api/shocktrade/stocks', {
    //////////////////////////////////////////////////////////////////////////////////////
    // creates a stock quote
    // http post 'http://{{host}}:{{port}}/api/shocktrade/stocks' <~ { symbol: "AAPL", exchange: "NASDAQ", lastSale: 98.11 }
    //////////////////////////////////////////////////////////////////////////////////////
    post: (symbol: String, exchange: String, lastSale: Double) => {
        val result = insert into StockQuotes (symbol, exchange, lastSale, lastSaleTime) values (@symbol, @exchange, @lastSale, DateTime())
        (result.inserted() is 1)
    }

    //////////////////////////////////////////////////////////////////////////////////////
    // retrieves a stock quote
    // http get 'http://{{host}}:{{port}}/api/shocktrade/stocks?symbol=AAPL'
    //////////////////////////////////////////////////////////////////////////////////////
    get: (symbol: String) => {
        from ns('StockQuotes') where symbol is @symbol limit 1
    },

    //////////////////////////////////////////////////////////////////////////////////////
    // updates a stock quote
    // http put 'http://{{host}}:{{port}}/api/shocktrade/stocks' <~ { symbol: "AAPL", lastSale: 98.87 }
    //////////////////////////////////////////////////////////////////////////////////////
    put: (symbol: String, lastSale: Double) => {
        update StockQuotes set lastSale = @lastSale, lastSaleTime = DateTime()
        where symbol is @symbol
    }
})