namespace 'demos.websockets'
//////////////////////////////////////////////////////////////////////////////////////
//      WEBSOCKETS DEMO
// include('./app/examples/src/main/lollypop/WebSocketsDemo.sql')
//////////////////////////////////////////////////////////////////////////////////////

def drawProgressMeter(total, processed, ratio = 0, rate = 0.0) := {
    val progressBarWidth = 50
    val progress = Int(ratio * progressBarWidth)
    val progressBar = "[" + ("»" * progress) + (" " * (progressBarWidth - progress)) + '] {{processed}}/{{total}} ({{ Int(ratio * 100) }}%) ~ {{rate}} rec/sec'
    stdout <=== '\r{{progressBar}}'
    ()
}

def updateProgressMeter(total, processed, response = "{}") := {
    val ratio = Double(received) / transactions
    val delta = System.currentTimeMillis() - startTime
    val rate = scaleTo(iff(delta > 0, received / (delta / 1000.0), Double(received)), 1)
    drawProgressMeter(total, processed, ratio, rate)
}

//////////////////////////////////////////////////////////////////////////////////////
// BUILD THE DATAFRAMES
//////////////////////////////////////////////////////////////////////////////////////

// create a dataframe (table) representing the input (ws_stocks.csv)
create external table ws_stocks_csv (symbol: String(10), exchange: String(10), lastSale: Double, lastSaleTime: DateTime)
  containing { format: 'csv', location: 'app/core/src/test/resources/ws_stocks.csv', headers: true, delimiter: ',' }

// (re-)create an indexed dataframe (table) to store the stock quotes
drop if exists ws_stocks
create table ws_stocks (symbol: String(10), exchange: String(10), lastSale: Double, lastSaleTime: DateTime)
  containing (from ns('ws_stocks_csv'))
create index ws_stocks#symbol

//////////////////////////////////////////////////////////////////////////////////////
// STARTUP A PEER NODE W/WEBSOCKETS
//////////////////////////////////////////////////////////////////////////////////////

node = Nodes.start()
node.api('/ws/websockets', {
    ws: message => {
        val js = message.fromJson()
        upsert into ws_stocks (symbol, exchange, lastSale, lastSaleTime)
        values (js.symbol, js.exchange, js.lastSale, js.lastSaleTime)
        where symbol is js.symbol
        true
    }
})

//////////////////////////////////////////////////////////////////////////////////////
// MAIN SECTION
//////////////////////////////////////////////////////////////////////////////////////

// setup the process to transmit data via websockets
var received = 0
stocks = ns('ws_stocks')
transactions = Int(stocks.getLength())
startTime = System.currentTimeMillis()

// notify the operator once the transfer completes
whenever received >= transactions
    once after '1 second'
        stdout <=== '\nTransfer completed in {{ (System.currentTimeMillis() - startTime)/1000.0 }} seconds.\n'

// establish the websocket connection
port = node.port
stdout <=== 'Connecting to ws://127.0.0.1:{{port}}/...\n'
ws = WebSockets('127.0.0.1', port, response => { received += 1; updateProgressMeter(transactions, received, response) })
ws.awaitConnection()

// iterate the stock quotes; sending each one to the websocket for updating.
stdout <=== 'Preparing transfer. Please standby...\n'
after '1 second' drawProgressMeter(transactions, received)
stocks.foreach(r => ws.send({
    id: r.id
    symbol: r.symbol
    exchange: r.exchange
    lastSale: r.lastSale
    lastSaleTime: DateTime()
}))
