package com.lollypop.examples

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.ws.{Message, TextMessage, WebSocketRequest}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.{Materializer, OverflowStrategy}
import akka.{Done, NotUsed}
import com.lollypop.die
import com.lollypop.language._
import com.lollypop.runtime._
import com.lollypop.runtime.devices.{Field, RowCollection}
import com.lollypop.util.StringRenderHelper.StringRenderer
import lollypop.lang.Random
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

import java.util.Date
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

class WebSocketsStudyTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)

  describe("WebSocketsStudy") {

    it("all examples should compile") {
      // create a web service and a table to capture the data
      val (sa, _, _) =
        """|namespace "demos.shocktrade"
           |
           |def generateStocks(qty: Int) := {
           |  [1 to qty].map(_ => {
           |      exchange = ['AMEX', 'NASDAQ', 'NYSE', 'OTCBB', 'OTHER_OTC'][Random.nextInt(5)]
           |      is_otc = exchange.startsWith("OT")
           |      lastSale = scaleTo(iff(is_otc, 1, 201) * Random.nextDouble(1.0), 4)
           |      lastSaleTime = DateTime(DateTime() - Duration(1000 * 60 * Random.nextDouble(1.0)))
           |      symbol = Random.nextString(['A' to 'Z'], iff(is_otc, Random.nextInt(2) + 4, Random.nextInt(4) + 2))
           |      select lastSaleTime, lastSale, exchange, symbol
           |  }).toTable()
           |}
           |
           |// create the ws_stocks table
           |drop if exists ws_stocks
           |create table ws_stocks (symbol: String(10), exchange: String(10), lastSale: Double, lastSaleTime: DateTime)
           |    containing (generateStocks(5000))
           |create index ws_stocks#symbol
           |
           |// start the websocket listener
           |node = Nodes.start()
           |node.awaitStartup(Duration('1 second'))
           |port = node.port
           |node.api('/ws/shocktrade', {
           |  ws: (message: String) => {
           |    stdout <=== message
           |    val js = message.fromJson()
           |    return upsert into ws_stocks (symbol, exchange, lastSale, lastSaleTime)
           |           values (js.symbol, js.exchange, js.lastSale, js.lastSaleTime)
           |           where symbol is js.symbol
           |  }
           |})
           |""".stripMargin.executeSQL(Scope())

      // get the node's listener port
      val port = sa.resolveAs[Int]("port") || die("'port' not found")
      logger.info(s"port: $port")

      // get a reference to the ws_stocks table
      val stockQuotes = sa.getRowCollection(DatabaseObjectRef("ws_stocks"))

      // start the websocket client
      implicit val system: ActorSystem = ActorSystem()
      implicit val materializer: Materializer = Materializer.matFromSystem
      startClient(port, stockQuotes)
    }

  }

  def wsConnect(host: String = "127.0.0.1", port: Int)(implicit system: ActorSystem, materializer: Materializer): (ActorRef, Future[_]) = {
    import system.dispatcher
    val req = WebSocketRequest(uri = s"ws://$host:$port/ws")
    val webSocketFlow = Http().webSocketClientFlow(req)
    val messageSource: Source[Message, ActorRef] =
      Source.actorRef[TextMessage.Strict](bufferSize = 10, OverflowStrategy.fail)
    val messageSink: Sink[Message, NotUsed] =
      Flow[Message]
        .map(message => println(s"Received text message: [$message]"))
        .to(Sink.ignore)

    val ((ws, upgradeResponse), closed) =
      messageSource
        .viaMat(webSocketFlow)(Keep.both)
        .toMat(messageSink)(Keep.both)
        .run()
    
    (ws, upgradeResponse.flatMap {
      case upgrade if upgrade.response.status == StatusCodes.SwitchingProtocols => Future.successful(Done)
      case upgrade => Future.failed(new RuntimeException(s"Connection failed: ${upgrade.response.status}"))
    })
  }

  def startClient(port: Int, stockQuotes: RowCollection)(implicit system: ActorSystem, materializer: Materializer): Unit = {
    val (ws, connected) = wsConnect(port = port)
    Await.ready(connected, 10.seconds)

    // send random stock quotes
    for (_ <- 1 to 25) {
      val rowID = Random.nextLong(stockQuotes.getLength)
      val row = stockQuotes(rowID)
      val newRow = row.copy(fields = row.fields.map {
        case f@Field("lastSale", _, Some(lastSale: Double)) => f.copy(value = Some(lastSale + Random.nextDouble()))
        case f@Field("lastSaleTime", _, _) => f.copy(value = Some(new Date()))
        case f => f
      })
      logger.info("")
      logger.info(s"BEFORE: [$rowID] ${row.toMap.renderAsJson}")
      logger.info(s"AFTER:  [$rowID] ${newRow.toMap.renderAsJson}")
      ws ! TextMessage.Strict(newRow.toMap.renderAsJson)
    }
  }

}
