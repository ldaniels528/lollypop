package lollypop.io

import com.lollypop.runtime.LollypopVM.implicits.LollypopVMSQL
import com.lollypop.runtime.Scope
import org.scalatest.funspec.AnyFunSpec

class WebSocketsTest extends AnyFunSpec {

  describe(classOf[WebSockets].getSimpleName) {

    it("all examples should compile") {
      // create a web service and a table to capture the data
      val (_, _, va) =
        """|namespace "demos.websockets"
           |
           |// create a dataframe from the .csv file (ws_stocks.csv)
           |create external table ws_stocks_csv (symbol: String(10), exchange: String(10), lastSale: Double, lastSaleTime: DateTime)
           |  containing { format: 'CSV', location: 'app/core/src/test/resources/ws_stocks.csv', headers: true, delimiter: ',' }
           |
           |// create the ws_stocks table
           |drop if exists ws_stocks
           |create table ws_stocks (symbol: String(10), exchange: String(10), lastSale: Double, lastSaleTime: DateTime)
           |  containing (from ns("ws_stocks_csv"))
           |create index ws_stocks#symbol
           |
           |// start the websocket listener
           |node = Nodes.start()
           |node.api("/ws/websockets", {
           |  ws: (message: String) => {
           |    stdout <=== ("server: " + message + "\n")
           |    val js = message.fromJson()
           |    return upsert into ws_stocks (symbol, exchange, lastSale, lastSaleTime)
           |           values (js.symbol, js.exchange, js.lastSale, js.lastSaleTime)
           |           where symbol is js.symbol
           |  }
           |})
           |
           |// connect to the node via websocket
           |port = node.port
           |stdout <=== "Connecting to 127.0.0.1:{{port}}...\n"
           |ws = WebSockets("127.0.0.1", port, message => stdout <=== "client ACK: " + message)
           |ws.awaitConnection()
           |
           |// update 15 stock quotes
           |import "java.lang.Thread"
           |stdout <=== "Preparing transfer. Please standby...\n"
           |stocks = ns("ws_stocks")
           |[1 to 10].foreach(n => {
           |  val rowID = Random.nextLong(stocks.getLength())
           |  val r = stocks[rowID]
           |  val msg = { id: r.id, symbol: r.symbol, exchange: r.exchange, lastSale: r.lastSale, lastSaleTime: DateTime() }
           |  ws.send(msg)
           |
           |  // slow it down to see all messages
           |  Thread.sleep(Long(50))
           |  stdout <=== ("client: " + msg + "\n")
           |})
           |""".stripMargin.executeSQL(Scope())
      assert(va == ())
    }

  }

}
