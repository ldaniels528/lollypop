package com.qwery.runtime.instructions.functions

import com.qwery.runtime.devices.RowCollectionZoo.ProductToRowCollection
import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.{QweryVM, Scope}
import com.qwery.util.DateHelper
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

class NodeAPITest extends AnyFunSpec with VerificationTools {
  private val logger = LoggerFactory.getLogger(getClass)

  describe(classOf[NodeAPI].getSimpleName) {

    it("should setup and execute a REST endpoint") {
      val (scopeA, responseA) = QweryVM.infrastructureSQL(Scope(),
        """|namespace 'demo.subscriptions'
           |
           |val port = nodeStart()
           |drop if exists subscriptions &&
           |create table subscriptions(id: RowNumber, name: String(64), startTime: DateTime, stopTime: DateTime)
           |""".stripMargin)
      responseA.toRowCollection.tabulate().foreach(logger.info)
      assert(responseA.created == 1)

      val (scopeB, _, responseB) = QweryVM.executeSQL(scopeA,
        """|nodeAPI(port, '/api/demo/subscriptions', {
           |  post: (name: String, startTime: DateTime) => {
           |    insert into subscriptions (name, startTime) values (@name, @startTime)
           |  },
           |  get: (id: Long) => {
           |    select * from subscriptions where id is @id
           |  },
           |  put: (id: Long, newName: String) => {
           |    update subscriptions set name = @newName where id is @id
           |  },
           |  delete: (id: Long, expiry: DateTime) => {
           |    update subscriptions set stopTime = @expiry where id is @id
           |  }
           |})
           |""".stripMargin)
      assert(responseB == true)

      val (scopeC, _, responseC) = QweryVM.searchSQL(scopeB,
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

      val (scopeD, _, responseD) = QweryVM.executeSQL(scopeC,
        """|val response = http get 'http://0.0.0.0:{{port}}/api/demo/subscriptions?id=1'
           |response.body.toJsonString()
           |""".stripMargin)
      logger.info(s"response: $responseD")
      assert(responseD == """[{"id":1,"name":"HBO Max","startTime":"2022-09-04T23:36:47.321Z"}]""")

      val (scopeE, _, responseE) = QweryVM.searchSQL(scopeD,
        """|http delete 'http://0.0.0.0:{{port}}/api/demo/subscriptions' <~ { id: 2, expiry: DateTime('2022-09-16T13:17:59.128Z') }
           |select * from subscriptions where id is 2
           |""".stripMargin)
      responseE.tabulate().foreach(logger.info)
      assert(responseE.toMapGraph == List(
        Map("id" -> 2, "name" -> "Showtime", "startTime" -> DateHelper("2022-09-04T23:36:48.503Z"), "stopTime" -> DateHelper("2022-09-16T13:17:59.128Z"))
      ))

      val (_, _, responseF) = QweryVM.searchSQL(scopeE,
        """|http put 'http://0.0.0.0:{{port}}/api/demo/subscriptions?id=3&newName=AmazonPrimeVideo'
           |select * from subscriptions where id is 3
           |""".stripMargin)
      responseF.tabulate().foreach(logger.info)
      assert(responseF.toMapGraph == List(
        Map("id" -> 3, "name" -> "AmazonPrimeVideo", "startTime" -> DateHelper("2022-09-04T23:36:48.504Z"))
      ))
    }

  }

}
