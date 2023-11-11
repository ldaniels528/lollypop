package com.lollypop.runtime.instructions.functions

import com.lollypop.language.LollypopUniverse
import com.lollypop.runtime.devices.RowCollectionZoo.ProductToRowCollection
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.{LollypopVM, Scope}
import com.lollypop.util.DateHelper
import lollypop.io.Nodes
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

import scala.concurrent.duration.DurationInt

class NodesTest extends AnyFunSpec with VerificationTools {
  private val logger = LoggerFactory.getLogger(getClass)
  private val ctx = LollypopUniverse(isServerMode = true)
  private val nodes = new Nodes(ctx)

  describe(classOf[Nodes].getSimpleName) {

    it("should start/stop local/remote peers") {
      // start 3 nodes
      val (nodeA, nodeB, nodeC) = (nodes.start(), nodes.start(), nodes.start())
      nodeA.awaitStartup(5.seconds)
      nodeB.awaitStartup(5.seconds)
      nodeC.awaitStartup(5.seconds)

      // there should be 3 peers
      assert(nodes.peers.length == 3)

      // ensure peers via port numbers
      assert(nodes.peers.flatMap(nodes.getNode).length == 3)

      // stop the peers
      nodes.peerNodes.foreach(_.stop())
    }

    it("should execute commands on a local/remote peer") {
      val (_, _, result) = LollypopVM.searchSQL(Scope(),
        """|node = Nodes.start()
           |node.awaitStartup(Interval('1 second'))
           |results = node.exec('''
           |from (
           ||-------------------------------------------------------|
           || ticker | market | lastSale | lastSaleTime             |
           ||-------------------------------------------------------|
           || NKWI   | OTCBB  |  98.9501 | 2022-09-04T23:36:47.846Z |
           || AQKU   | NASDAQ |  68.2945 | 2022-09-04T23:36:47.860Z |
           || WRGB   | AMEX   |  46.8355 | 2022-09-04T23:36:47.862Z |
           || ESCN   | AMEX   |  42.5934 | 2022-09-04T23:36:47.865Z |
           || NFRK   | AMEX   |  28.2808 | 2022-09-04T23:36:47.864Z |
           ||-------------------------------------------------------|
           |) where lastSale < 30
           |''')
           |node.stop()
           |results
           |""".stripMargin)
      result.tabulate().foreach(logger.info)
      assert(result.toMapGraph == List(
        Map("market" -> "AMEX", "lastSale" -> 28.2808, "lastSaleTime" -> DateHelper("2022-09-04T23:36:47.864Z"), "ticker" -> "NFRK")
      ))
    }

    it("should execute commands on a node from Scala") {
      val node = nodes.start()
      val result = node.exec(
        """|from (
           ||-------------------------------------------------------|
           || ticker | market | lastSale | lastSaleTime             |
           ||-------------------------------------------------------|
           || NKWI   | OTCBB  |  98.9501 | 2022-09-04T23:36:47.846Z |
           || AQKU   | NASDAQ |  68.2945 | 2022-09-04T23:36:47.860Z |
           || WRGB   | AMEX   |  46.8355 | 2022-09-04T23:36:47.862Z |
           || ESCN   | AMEX   |  42.5934 | 2022-09-04T23:36:47.865Z |
           || NFRK   | AMEX   |  28.2808 | 2022-09-04T23:36:47.864Z |
           ||-------------------------------------------------------|
           |) where lastSale < 30
           |""".stripMargin)
      result.tabulate().foreach(logger.info)
      assert(result.toRowCollection.toMapGraph == List(
        Map("market" -> "AMEX", "lastSale" -> 28.2808, "lastSaleTime" -> DateHelper("2022-09-04T23:36:47.864Z"), "ticker" -> "NFRK")
      ))
      node.stop()
    }

    it("should execute commands on multiple local/remote peers") {
      val (_, _, result) = LollypopVM.searchSQL(Scope(),
        s"""|# create a public macro
            |drop if exists stocksMacro
            |create macro stocksMacro := 'STOCKS %e:total' {
            |    declare table myQuotes(symbol: String(4), exchange: String(6), lastSale: Double, lastSaleTime: DateTime)
            |    var n: Int = 0
            |    while n < total {
            |        n += 1
            |        insert into @myQuotes (lastSaleTime, lastSale, exchange, symbol)
            |        select lastSaleTime: DateTime(),
            |        lastSale: scaleTo(500 * Random.nextDouble(0.99), 4),
            |        exchange: ['AMEX', 'NASDAQ', 'NYSE', 'OTCBB'][Random.nextInt(4)],
            |        symbol: Random.nextString(['A' to 'Z'], 4)
            |    }
            |    @myQuotes
            |}
            |
            |// start the remote peers
            |val nodeA = Nodes.start()
            |val nodeB = Nodes.start()
            |nodeA.awaitStartup(Interval('1 second'))
            |nodeB.awaitStartup(Interval('1 second'))
            |
            |stocksA = nodeA.exec("STOCKS 3")
            |stocksB = nodeB.exec("STOCKS 3")
            |results = @stocksA & @stocksB
            |
            |nodeA.stop()
            |nodeB.stop()
            |results
            |""".stripMargin)
      result.tabulate().foreach(logger.info)
    }

    it("should setup and execute a REST endpoint") {
      val (scopeA, responseA, _) = LollypopVM.executeSQL(Scope(),
        """|namespace 'demo.subscriptions'
           |node = Nodes.start()
           |node.awaitStartup(Interval('1 second'))
           |port = node.port
           |drop if exists subscriptions &&
           |create table subscriptions(id: RowNumber, name: String(64), startTime: DateTime, stopTime: DateTime)
           |""".stripMargin)
      responseA.toRowCollection.tabulate().foreach(logger.info)
      assert(responseA.created == 1)

      val (scopeB, _, responseB) = LollypopVM.executeSQL(scopeA,
        s"""|node.api('/api/demo/subscriptions', {
            |  post: (name: String, startTime: DateTime) => {
            |    insert into subscriptions (name, startTime) values ($$name, $$startTime)
            |  },
            |  get: (id: Long) => {
            |    select * from subscriptions where id is $$id
            |  },
            |  put: (id: Long, newName: String) => {
            |    update subscriptions set name = $$newName where id is $$id
            |  },
            |  delete: (id: Long, expiry: DateTime) => {
            |    update subscriptions set stopTime = $$expiry where id is $$id
            |  }
            |})
            |""".stripMargin)
      assert(responseB == true)

      val (scopeC, _, responseC) = LollypopVM.searchSQL(scopeB,
        """|http post 'http://0.0.0.0:{{port}}/api/demo/subscriptions' <~ { name: 'ABC News', startTime: DateTime('2022-09-04T23:36:46.862Z') }
           |http post 'http://0.0.0.0:{{port}}/api/demo/subscriptions' <~ { name: 'HBO Max', startTime: DateTime('2022-09-04T23:36:47.321Z') }
           |http post 'http://0.0.0.0:{{port}}/api/demo/subscriptions' <~ { name: 'Showtime', startTime: DateTime('2022-09-04T23:36:48.503Z') }
           |http post 'http://0.0.0.0:{{port}}/api/demo/subscriptions?name=IMDB' <~ { startTime: DateTime('2022-09-04T23:36:48.504Z') }
           |select * from subscriptions
           |""".stripMargin)
      responseC.tabulate().foreach(logger.info)
      assert(responseC.toMapGraph == List(
        Map("id" -> 0, "name" -> "ABC News", "startTime" -> DateHelper("2022-09-04T23:36:46.862Z")),
        Map("id" -> 1, "name" -> "HBO Max", "startTime" -> DateHelper("2022-09-04T23:36:47.321Z")),
        Map("id" -> 2, "name" -> "Showtime", "startTime" -> DateHelper("2022-09-04T23:36:48.503Z")),
        Map("id" -> 3, "name" -> "IMDB", "startTime" -> DateHelper("2022-09-04T23:36:48.504Z"))
      ))

      val (scopeD, _, responseD) = LollypopVM.executeSQL(scopeC,
        """|val response = http get 'http://0.0.0.0:{{port}}/api/demo/subscriptions?id=1'
           |response.body.toJsonString()
           |""".stripMargin)
      logger.info(s"response: $responseD")
      assert(responseD == """[{"id":1,"name":"HBO Max","startTime":"2022-09-04T23:36:47.321Z"}]""")

      val (scopeE, _, responseE) = LollypopVM.searchSQL(scopeD,
        """|http delete 'http://0.0.0.0:{{port}}/api/demo/subscriptions' <~ { id: 2, expiry: DateTime('2022-09-16T13:17:59.128Z') }
           |select * from subscriptions where id is 2
           |""".stripMargin)
      responseE.tabulate().foreach(logger.info)
      assert(responseE.toMapGraph == List(
        Map("id" -> 2, "name" -> "Showtime", "startTime" -> DateHelper("2022-09-04T23:36:48.503Z"), "stopTime" -> DateHelper("2022-09-16T13:17:59.128Z"))
      ))

      val (_, _, responseF) = LollypopVM.searchSQL(scopeE,
        """|http put 'http://0.0.0.0:{{port}}/api/demo/subscriptions?id=3&newName=AmazonPrimeVideo'
           |select * from subscriptions where id is 3
           |""".stripMargin)
      responseF.tabulate().foreach(logger.info)
      assert(responseF.toMapGraph == List(
        Map("id" -> 3, "name" -> "AmazonPrimeVideo", "startTime" -> DateHelper("2022-09-04T23:36:48.504Z"))
      ))
    }

    it("should execute a statement on a local/remote peer") {
      val (_, _, r) = LollypopVM.searchSQL(Scope(),
        """|node = Nodes.start()
           |node.awaitStartup(Interval('1 second'))
           |try
           |  node.exec('''
           |    x = 1
           |    y = 2
           |    z = x + y
           |    z
           |  ''')
           |catch e => stderr <=== e
           |finally
           |  node.stop()
           |""".stripMargin)
      assert(r.toMapGraph == List(Map("result" -> 3)))
    }

    it("should execute an inline script on a local/remote peer") {
      val (_, _, r) = LollypopVM.searchSQL(Scope(),
        """|node = Nodes.start()
           |node.awaitStartup(Interval('1 second'))
           |try
           |  node.exec([
           |    "x = 1",
           |    "y = 2",
           |    "z = x + y",
           |    "z"
           |  ])
           |catch e => stderr <=== e
           |finally
           |  node.stop()
           |""".stripMargin)
      assert(r.toMapGraph == List(Map("result" -> 3)))
    }

    it("should execute an inline script on a node from Scala") {
      val node = nodes.start()
      val response = try {
        node.interact(console = {
          val it = Seq(
            "x = 3",
            "y = 6",
            "z = x + y",
            "z"
          ).iterator
          () => if (it.hasNext) it.next() else "exit"
        })
      } finally node.stop()
      assert(response.toList.flatMap(_.toRowCollection.toMapGraph) == List(Map("result" -> 9)))
    }

  }

}
